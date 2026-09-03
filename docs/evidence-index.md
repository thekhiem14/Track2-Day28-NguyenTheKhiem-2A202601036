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
| IP01 | HTTP → Kafka | **PASS** | 13 document + 5 feedback `accepted` qua gateway, mỗi phản hồi có `event_id`, `idempotency_key`, `topic=data.raw`, `trace_id`; `data.raw` có 86 message trên 3 partition |
| IP02 | Kafka → Airflow 3 | **PASS** | DAG `lab28_ingestion_pipeline` run `manual-29d85b52` `success`, cả 4 task xanh; `polled: 86, processed: 22, dead_lettered: 0` |
| IP03 | Airflow/Spark → Delta | **PASS** | `feedback` version 1 / 8 rows, `documents` version 1 / 14 rows, `last_operation: MERGE` |
| IP04 | Delta → Feast | **PASS** | Feast `/health` 200; task `refresh_online_features` success |
| IP05 | Delta → Qdrant | **PASS** | collection `lab28_documents`, 13 → 14 points, embedding model ghim kèm revision |
| IP06 | Eval → MLflow Registry | **PASS** | `lab28-rag-release` v2 giữ alias `champion`, có `run_id`, prompt version, model id |
| IP07 | RAG → vLLM thật | **UNVERIFIED (gate: gpu)** | `probe_identity`: `unreachable: ConnectError`, `is_real_vllm: false`. Máy làm bài không có GPU. Cần endpoint vLLM thật (Kaggle/cluster) rồi chạy lại `lab28 ready` |
| IP08 | Client → Envoy | **PASS** | `200` kèm `x-request-id` và `x-envoy-decorator-operation: lab28.gateway.request`; burst seed bị chặn `429 local_rate_limited` |
| IP09 | → Prometheus/Grafana | **PASS** | 9/10 target `up`; chỉ `lab28-vllm-optional` `down` (đúng như thiết kế khi không có GPU) |
| IP10 | → OTLP trace | **PARTIAL** | Jaeger có service `lab28-api`, `lab28-gateway`; đã thấy `lab28.gateway.request`, `lab28.api.ingest`, `lab28.kafka.produce`, `lab28.airflow.dag`, `lab28.mlflow.resolve_release`. Các span cần vLLM chưa có |

`UNVERIFIED` của IP07 và nhánh LangSmith của IP10 là **gate theo môi trường**, đúng
như `contracts/integration-matrix.yaml` mô tả, không phải lỗi cài đặt.

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
