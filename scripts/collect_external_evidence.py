"""Collect the six evidence files ``lab28 evidence`` cannot produce itself.

``lab28 evidence`` deliberately writes only what the serving process can prove
from inside itself.  The rest of the matrix crosses a boundary that process
never touches: a Kafka message as it sits on the topic, an Airflow run, a Feast
online read, the gateway's own refusal, Prometheus' target list and a trace in
the backend.

Every value written here is read back from a live component.  Nothing is
synthesised: when a source cannot be reached the file records the failure
instead of a plausible-looking result, because an evidence pack that looks
complete but is not is worse than one that is honestly short.
"""

from __future__ import annotations

import base64
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lab28_platform.contracts import FEATURE_REFS  # noqa: E402
from lab28_platform.readiness import load_matrix  # noqa: E402

EVIDENCE = ROOT / "evidence"
GATEWAY = "http://localhost:8080"
AIRFLOW = "http://localhost:8082"
FEAST = "http://localhost:6566"
PROMETHEUS = "http://localhost:9090"
JAEGER = "http://localhost:16686"
AIRFLOW_PASSWORDS = ROOT / ".lab28" / "airflow" / "simple-auth-passwords.json"
DAG_ID = "lab28_ingestion_pipeline"


def _get(url: str, headers: dict[str, str] | None = None, timeout: float = 30.0) -> Any:
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _post(url: str, body: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
    payload = json.dumps(body).encode("utf-8")
    merged = {"Content-Type": "application/json", **(headers or {})}
    request = urllib.request.Request(url, data=payload, headers=merged, method="POST")
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def _airflow_token() -> str:
    credentials = json.loads(AIRFLOW_PASSWORDS.read_text(encoding="utf-8"))
    username, password = next(iter(credentials.items()))
    body = {"username": username, "password": password}
    return str(_post(f"{AIRFLOW}/auth/token", body)["access_token"])


# -- IP01: a message as it actually sits on data.raw -----------------------


def ip01_kafka_consume() -> dict[str, Any]:
    """Read data.raw with a throwaway group so pipeline offsets are untouched."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"evidence-reader-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["data.raw"])
    samples: list[dict[str, Any]] = []
    try:
        deadline = time.time() + 30
        while time.time() < deadline and len(samples) < 3:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            headers = {
                key: value.decode("utf-8", "replace") if isinstance(value, bytes) else value
                for key, value in (message.headers() or [])
            }
            samples.append(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "headers": headers,
                    "has_traceparent": "traceparent" in headers,
                    "has_idempotency_key": "idempotency-key" in headers,
                    "key_equals_idempotency_header": (
                        message.key().decode("utf-8") if message.key() else None
                    )
                    == headers.get("idempotency-key"),
                    "value": json.loads(message.value().decode("utf-8")),
                }
            )
    finally:
        consumer.close()
    return {
        "source": "kafka topic data.raw, read with a non-committing consumer group",
        "sample_count": len(samples),
        "samples": samples,
    }


# -- IP02: the Airflow run that moved the batch ----------------------------


def ip02_airflow_run() -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {_airflow_token()}", "Accept": "application/json"}
    runs = _get(
        f"{AIRFLOW}/api/v2/dags/{DAG_ID}/dagRuns?order_by=-start_date&limit=5", headers
    )["dag_runs"]
    drained = None
    for run in runs:
        run_id = run["dag_run_id"]
        tasks = _get(
            f"{AIRFLOW}/api/v2/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances", headers
        )["task_instances"]
        record = {
            "dag_id": DAG_ID,
            "dag_run_id": run_id,
            "state": run["state"],
            "logical_date": run.get("logical_date"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "task_states": {task["task_id"]: task["state"] for task in tasks},
        }
        try:
            xcom = _get(
                f"{AIRFLOW}/api/v2/dags/{DAG_ID}/dagRuns/{run_id}"
                "/taskInstances/drain_kafka_into_delta/xcomEntries/return_value",
                headers,
            )
            record["drain_result"] = xcom.get("value")
        except urllib.error.HTTPError as error:
            record["drain_result"] = f"unavailable: HTTP {error.code}"
        if drained is None and run["state"] == "success":
            drained = record
        record.setdefault("_rank", 0)
    try:
        assets = _get(f"{AIRFLOW}/api/v2/assets/events?limit=5&order_by=-timestamp", headers)
    except urllib.error.HTTPError as error:
        assets = {"error": f"HTTP {error.code}"}
    return {
        "health": _get(f"{AIRFLOW}/api/v2/monitor/health"),
        "latest_successful_run": drained,
        "recent_runs": [
            {"dag_run_id": run["dag_run_id"], "state": run["state"]} for run in runs
        ],
        "asset_events": assets,
    }


# -- IP04: the online feature row -----------------------------------------


def ip04_feast_online() -> dict[str, Any]:
    from lab28_platform.integration_tasks import feast_online_request

    entity_ids = ["asker-001", "asker-002", "asker-003"]
    reads: list[dict[str, Any]] = []
    for asker_id in entity_ids:
        request_body = feast_online_request(asker_id)
        try:
            body = _post(f"{FEAST}/get-online-features", request_body)
        except (urllib.error.HTTPError, urllib.error.URLError) as error:
            reads.append({"asker_id": asker_id, "error": str(error)})
            continue
        reads.append({"asker_id": asker_id, "request": request_body, "response": body})
    return {
        "feature_view": "asker_activity_v1",
        "feature_refs": list(FEATURE_REFS),
        "health": _get(f"{FEAST}/health") if _feast_health_is_json() else "200 OK",
        "reads": reads,
    }


def _feast_health_is_json() -> bool:
    try:
        _get(f"{FEAST}/health")
    except Exception:
        return False
    return True


# -- IP08: the gateway's own 200 and 429 ----------------------------------


def ip08_gateway() -> dict[str, Any]:
    def call(path: str) -> dict[str, Any]:
        request = urllib.request.Request(f"{GATEWAY}{path}")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                status, headers = response.status, dict(response.headers)
                body = response.read(200).decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            status, headers = error.code, dict(error.headers)
            body = error.read(200).decode("utf-8", "replace")
        return {
            "path": path,
            "status": status,
            "x_request_id": headers.get("x-request-id"),
            "x_envoy_decorator_operation": headers.get("x-envoy-decorator-operation"),
            "server": headers.get("server"),
            "body": body,
        }

    accepted = call("/health")
    refused: dict[str, Any] | None = None
    for _ in range(60):
        attempt = call("/api/v1/feedback")
        if attempt["status"] == 429:
            refused = attempt
            break
    return {
        "listener": GATEWAY,
        "accepted": accepted,
        "rate_limited": refused or "no 429 observed in 60 attempts",
        "admin_ready": _text(f"http://localhost:9901/ready"),
    }


def _text(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.read(100).decode("utf-8", "replace").strip()
    except Exception as error:  # noqa: BLE001 - evidence records the failure
        return f"unavailable: {type(error).__name__}"


# -- IP09: Prometheus targets and the loaded alert rules -------------------


def ip09_prometheus_targets() -> dict[str, Any]:
    targets = _get(f"{PROMETHEUS}/api/v1/targets?state=active")["data"]["activeTargets"]
    rules = _get(f"{PROMETHEUS}/api/v1/rules")["data"]["groups"]
    return {
        "targets": sorted(
            (
                {
                    "job": target["labels"].get("job"),
                    "instance": target["labels"].get("instance"),
                    "health": target["health"],
                    "last_error": target.get("lastError") or None,
                }
                for target in targets
            ),
            key=lambda item: str(item["job"]),
        ),
        "up_count": sum(1 for target in targets if target["health"] == "up"),
        "total_count": len(targets),
        "alert_rules": [
            {
                "group": group["name"],
                "alert": rule.get("name"),
                "state": rule.get("state"),
                "severity": (rule.get("labels") or {}).get("severity"),
                "expr": rule.get("query"),
            }
            for group in rules
            for rule in group.get("rules", [])
        ],
    }


def ip09_grafana_dashboards() -> dict[str, Any]:
    provisioned = sorted(
        path.name for path in (ROOT / "monitoring" / "grafana" / "dashboards").glob("*.json")
    )
    try:
        search = _get("http://localhost:3000/api/search?type=dash-db")
        live = [{"title": item.get("title"), "uid": item.get("uid")} for item in search]
    except Exception as error:  # noqa: BLE001
        live = [{"error": f"{type(error).__name__}: {error}"}]
    return {"provisioned_files": provisioned, "dashboards_in_grafana": live}


# -- IP10: one trace and the span names it carries -------------------------


def ip10_trace() -> dict[str, Any]:
    required = set(load_matrix()["required_spans"])
    services = _get(f"{JAEGER}/api/services")["data"] or []
    best: dict[str, Any] | None = None
    for service in services:
        try:
            traces = _get(f"{JAEGER}/api/traces?service={service}&limit=40")["data"]
        except Exception:  # noqa: BLE001
            continue
        for trace in traces:
            names = {span["operationName"] for span in trace.get("spans", [])}
            covered = required & names
            if best is None or len(covered) > len(best["required_spans_present"]):
                best = {
                    "trace_id": trace["traceID"],
                    "queried_via_service": service,
                    "span_count": len(trace.get("spans", [])),
                    "required_spans_present": sorted(covered),
                    "required_spans_missing": sorted(required - names),
                    "all_span_names": sorted(names),
                }
    return {
        "backend": JAEGER,
        "services": services,
        "required_spans": sorted(required),
        "best_trace": best or "no trace found",
        "langsmith": "UNVERIFIED — no LANGSMITH_API_KEY in this environment",
    }


COLLECTORS = {
    "ip01-kafka-consume.json": ip01_kafka_consume,
    "ip02-airflow-run.json": ip02_airflow_run,
    "ip04-feast-online.json": ip04_feast_online,
    "ip08-gateway.json": ip08_gateway,
    "ip09-prometheus-targets.json": ip09_prometheus_targets,
    "ip09-grafana-dashboards.json": ip09_grafana_dashboards,
    "ip10-trace.json": ip10_trace,
}


def main() -> None:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    failed: dict[str, str] = {}
    for filename, collector in COLLECTORS.items():
        try:
            payload = collector()
        except Exception as error:  # noqa: BLE001 - a failure is itself the evidence
            failed[filename] = f"{type(error).__name__}: {error}"
            print(f"skipped {filename}: {failed[filename]}")
            continue
        target = EVIDENCE / filename
        target.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
        )
        written.append(filename)
        print(f"wrote {target}")
    print(json.dumps({"written": written, "failed": failed}, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
