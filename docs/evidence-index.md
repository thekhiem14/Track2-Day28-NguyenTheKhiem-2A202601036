# Evidence index — trạng thái thật của 10 integration point

Tài liệu này ghi **đúng những gì đã chạy được trên máy làm bài**, kèm lệnh để dựng
lại. Nguyên tắc: cái gì chưa chứng minh được thì ghi `UNVERIFIED` và nói rõ thiếu gì —
không suy đoán, không làm giả (theo `docs/rubric.md`, làm giả evidence là 0 điểm phần
tương ứng).

## Môi trường đã chạy

| Mục | Giá trị |
|---|---|
| OS | Windows 11, Docker Desktop (engine 29.2.0) |
| Phần cứng | 20 CPU, 16 GiB RAM, ~58 GiB trống |
| Profile Compose | `full` — 14 service, tất cả `running`, các service có healthcheck đều `healthy` |
| Không có | GPU cho vLLM; cluster Kubernetes; credential LangSmith |

Dựng lại:

```text
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
docker compose --env-file ports.template --profile full up -d --build --wait
uv run lab28 topics && uv run lab28 index --source file && uv run lab28 release
uv run lab28 seed --via-gateway
```

## Bảng trạng thái

| IP | Boundary | Trạng thái | Bằng chứng đã quan sát |
|---|---|---|---|
| IP01 | HTTP → Kafka | **PASS** | `evidence/ip01-kafka-consume.json`: message trên `data.raw` có `traceparent` + `idempotency-key`, và **key của message trùng đúng header `idempotency-key`** |
| IP02 | Kafka → Airflow 3 | **PASS** | `evidence/ip02-airflow-run.json`: DAG run `success`, cả 4 task xanh; `polled: 86, processed: 22, dead_lettered: 0` |
| IP03 | Airflow/Spark → Delta | **PASS** | `evidence/ip03-delta-history.json`: `feedback` v1 / 8 rows, `documents` v1 / 14 rows, `last_operation: MERGE`, có time travel |
| IP04 | Delta → Feast | **PASS** | `evidence/ip04-feast-online.json`: `/get-online-features` trả `results` cho `asker-001/2/3` |
| IP05 | Delta → Qdrant | **PASS** | `evidence/ip05-qdrant-search.json`: collection `lab28_documents`, 14 points, embedding model ghim kèm revision |
| IP06 | Eval → MLflow Registry | **PASS** | `evidence/ip06-mlflow-release.json`: `lab28-rag-release` v2 giữ alias `champion` |
| IP07 | RAG → vLLM thật | xem mục "IP07" bên dưới | `evidence/ip07-vllm-identity.json` |
| IP08 | Client → Envoy | **PASS** | `evidence/ip08-gateway.json`: `200` và `429` **đều có `x-request-id` riêng**; `x-envoy-decorator-operation: lab28.gateway.request` |
| IP09 | → Prometheus/Grafana | **PASS** | `evidence/ip09-prometheus-targets.json`: 9/10 target `up` (chỉ `lab28-vllm-optional` down), 2 alert rule đã nạp: `Lab28ApiUnavailable`, `Lab28HighErrorRatio` |
| IP10 | → OTLP trace | xem mục "IP10" bên dưới | `evidence/ip10-trace.json` |

### Kết quả 5 critical journey

```text
uv run pytest integration-tests/test_j1_golden_path.py integration-tests/test_j2_idempotent_replay.py -q
  -> 21 passed, 3 skipped (chỉ skip do gate gpu)

uv run pytest integration-tests -m "not gpu and not langsmith" -q
  -> 56 passed, 16 deselected
```

Toàn bộ J1–J5 cộng gateway rate limit, Prometheus targets và trace span coverage đều đạt.

### Fast suite và các kiểm tra tĩnh

```text
uv run pytest starter-tests tests -q       -> 87 passed
uv run ruff check .                        -> All checks passed
uv run python scripts/verify_matrix.py     -> 245 checks passed
uv run python scripts/check_portability.py -> OK
uv run python scripts/validate_manifests.py-> passed
```

### Báo cáo tổng hợp

`evidence/integration-report.json` (sinh bởi `lab28 integration`) chấm **từ trong tiến
trình serving**, nên IP02/IP08/IP09/IP10 hiện `unverified` — đúng theo thiết kế: process
này không tự nhìn thấy Airflow, gateway, Prometheus hay trace backend. Bốn điểm đó được
chứng minh bằng các file evidence tương ứng ở bảng trên, thu bởi
`scripts/collect_external_evidence.py` đọc trực tiếp từ từng thành phần.

## IP07 — vLLM thật

Trạng thái: **PENDING** (đang dựng). Máy làm bài **có** GPU NVIDIA RTX 3060 Laptop
(6 GiB VRAM) và Docker đã truy cập được GPU:

```text
docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi   -> thấy RTX 3060
```

Cách chạy:

```text
docker compose --env-file ports.template -f compose.yaml -f compose.gpu.yaml \
  --profile full --profile gpu up -d vllm
uv run lab28 ready
uv run pytest integration-tests -m gpu -q
```

Ràng buộc cần biết: VRAM 6 GiB và màn hình đã chiếm ~1 GiB, nên chỉ vừa model nhỏ.
`ports.template` đang đặt `LAB28_VLLM_MODEL_ID=Qwen/Qwen3-1.7B` là lựa chọn phù hợp.

Gate không thể lách: `probe_identity` đòi **đồng thời** `/version` của chính vLLM và
metric family `vllm:`. Một server chỉ bắt chước OpenAI API sẽ trượt.

## IP10 — trace liên tục

Trạng thái: **PARTIAL**. `evidence/ip10-trace.json` ghi trace tốt nhất tìm được trong
Jaeger: **6/11** span bắt buộc.

Đã có: `lab28.gateway.request`, `lab28.api.ingest`, `lab28.kafka.produce`,
`lab28.kafka.consume`, `lab28.airflow.dag`, `lab28.spark.delta_merge` — tức là **toàn
bộ nhánh ingestion** đã liên tục từ gateway tới Delta.

Còn thiếu 5 span, tất cả đều nằm trên đường `/ask`: `lab28.api.ask`,
`lab28.feast.get_online_features`, `lab28.qdrant.query`, `lab28.mlflow.resolve_release`,
`lab28.vllm.chat_completion`. Lý do là `/ask` chưa chạy trọn vẹn được khi vLLM chưa bật —
đúng theo thiết kế ở `pipeline.py`: inference là dependency **duy nhất không có degraded
path**, không có model thì không có câu trả lời. Sau khi IP07 bật, một lần `/ask` thành
công sẽ khép đủ 11 span.

Nhánh LangSmith của IP10: **UNVERIFIED** — không có `LANGSMITH_API_KEY` trong môi
trường. Đây là gate theo môi trường mà `contracts/integration-matrix.yaml` đã khai báo,
không phải lỗi cài đặt.

## Bằng chứng cho phần tự làm

Hai quan sát dưới đây là kết quả của bốn hàm trong `integration_tasks.py` khi chạy thật.

**`dedupe_latest` (IP03).** Log task `drain_kafka_into_delta`:

```text
polled: 86, processed: 22, dead_lettered: 0
merged 86 events into Delta: {'feedback': 8, 'documents': 14} (versions {'feedback': 1, 'documents': 1})
```

86 message thô gộp còn 22 khóa duy nhất trước khi vào `MERGE`. Đây chính là hàm
`dedupe_latest` đang chặn dữ liệu trùng do seed nhiều lần.

**`readiness_status` (IP07 + IP08).** Cùng một tập probe, chỉ khác cờ `mandatory`:

```text
uv run lab28 ready                                  -> not_ready  (vllm mandatory, đang down)
LAB28_VLLM_REQUIRE_REAL=false uv run lab28 ready    -> degraded   (vllm không bắt buộc)
```

Bốn probe còn lại (`kafka`, `mlflow`, `qdrant`, `feast`) `ready=True` trong cả hai lần.
Đây là bằng chứng trực tiếp cho việc phân biệt `degraded` với `not_ready`.

## Kubernetes / GitOps

Không có cluster trên máy làm bài, nên chỉ xác minh **tĩnh**:

```text
uv run python scripts/validate_manifests.py     -> passed
kubectl kustomize deploy/kubernetes/base        -> render 10 resource
```

10 kind: `Namespace`, `ServiceAccount`, `ConfigMap`, `Deployment`, `Service`,
`HorizontalPodAutoscaler`, `PodDisruptionBudget`, `NetworkPolicy`, `Gateway`,
`HTTPRoute`.

**Drift/self-heal và rollback trên cluster thật: `UNVERIFIED`** — cần kind/k3s + Argo CD.
Cơ chế rollback đã cấu hình sẵn: `gitops/application.yaml` ghim
`targetRevision: refs/tags/v3.0.0`, `selfHeal: true`, `prune: true`,
`revisionHistoryLimit: 5`; rollback là đưa tag về revision trước và để Argo CD sync.

## Hai lỗi môi trường gặp thật khi chạy

Ghi lại vì đây là loại lỗi facilitator hay hỏi, và cả hai đều **không** phải lỗi code.

**1. `UnicodeEncodeError` khi chạy `lab28 release` trên Windows.**
MLflow in emoji (`🏃`, `🧪`) ra stdout; console Windows dùng codepage `cp1252` nên
không encode được, làm lệnh dừng **sau khi** run đã tạo. Cách xử lý:

```text
$env:PYTHONUTF8 = "1"     # PowerShell
uv run lab28 release
```

Hệ quả cần biết khi demo: lần chạy hỏng vẫn đã đăng ký version 1, nên lần chạy sau ra
version 2. Điều này lại tiện cho phần rollback vì có sẵn hai version.

**2. DAG chạy `success` nhưng `polled: 0` ở lần đầu.**
Consumer group `lab28-pipeline` mới tạo cần thời gian join/rebalance, trong khi
`poll_batch` chỉ chờ `idle_polls=3 × poll_timeout=1.0s`. Lần đầu hết 3 giây trước khi
được gán partition nên không đọc được gì. Lần trigger thứ hai (group đã ổn định) đọc
đủ 86 message. Khi demo: kích hoạt DAG một lần cho group ổn định **trước** khi bắt đầu
trình bày, hoặc trigger lại nếu thấy `polled: 0`.

## Lệnh sinh evidence file

```text
uv run lab28 evidence
uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

`.gitignore` loại trừ thư mục `evidence/` (nó là output runtime), nên các file JSON này
được nộp kèm riêng chứ không commit vào repo.
