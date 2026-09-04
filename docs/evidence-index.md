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
| GPU suy luận | Kaggle T4 + cloudflare quick tunnel (tạm thời, đã hết hạn sau buổi chạy) |
| Không có | cluster Kubernetes; credential LangSmith |

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
| IP03 | Airflow/Spark → Delta | **PASS** | `evidence/ip03-delta-history.json`: `last_operation: MERGE`, version tăng đơn điệu, có time travel (lần chạy cuối: `feedback` v17/23 rows, `documents` v11/20 rows) |
| IP04 | Delta → Feast | **PASS** | `evidence/ip04-feast-online.json`: `/get-online-features` trả `results` cho `asker-001/2/3` |
| IP05 | Delta → Qdrant | **PASS** | `evidence/ip05-qdrant-search.json`: collection `lab28_documents`, 20 points, embedding model ghim kèm revision |
| IP06 | Eval → MLflow Registry | **PASS** | `evidence/ip06-mlflow-release.json`: `lab28-rag-release` v6 giữ alias `champion`, ghi đúng `vllm_model_id` |
| IP07 | RAG → vLLM thật | **PASS** (xác minh sống 2026-09-03, tunnel đã hết hạn — xem mục "IP07") | MLflow release v6 + span `lab28.vllm.chat_completion` |
| IP08 | Client → Envoy | **PASS** | `evidence/ip08-gateway.json`: `200` và `429` **đều có `x-request-id` riêng**; `x-envoy-decorator-operation: lab28.gateway.request` |
| IP09 | → Prometheus/Grafana | **PASS** | `evidence/ip09-prometheus-targets.json`: 9/10 target `up` (chỉ `lab28-vllm-optional` down), 2 alert rule đã nạp: `Lab28ApiUnavailable`, `Lab28HighErrorRatio` |
| IP10 | → OTLP trace | **PASS** | `evidence/ip10-trace.json`: trace `463242b83527480aa428478ce64a6c04`, 34 span, **11/11** span bắt buộc |

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

Trạng thái: **PASS**, xác minh sống ngày 2026-09-03 qua endpoint Kaggle T4 + cloudflare
quick tunnel. Tunnel là **tạm thời** nên đã hết hạn sau buổi chạy; vì vậy
`evidence/ip07-vllm-identity.json` hiện ghi `reachable: false` — đó là kết quả thật ở
thời điểm thu lại, **không** sửa tay thành `true`.

Ba tín hiệu gate đã quan sát trực tiếp khi endpoint còn sống:

```text
GET /version    -> {"version":"0.26.0"}
GET /v1/models  -> {"id":"Qwen/Qwen3-4B-Instruct-2507", "owned_by":"vllm", ...}
GET /metrics    -> vllm:estimated_flops_per_gpu_total{model_name="Qwen/Qwen3-4B-Instruct-2507"} ...
GET /health     -> 200
```

`lab28 ready` khi đó trả **`ready`**, cả 5 probe xanh, dòng vLLM ghi
`vLLM identity confirmed`.

Một lần `/ask` qua gateway chạy trọn vẹn:

```text
question   : "Phan biet /health, /ready va /startup khac nhau the nao?"
answer     : có trích dẫn nguồn [1], nội dung đúng
trace_id   : f5d7220c29aa27e3f302d8dbbcadaf10
model      : Qwen/Qwen3-4B-Instruct-2507
mlflow     : v6
degraded   : False
sources    : doc-health-semantics, doc-ip08-gateway, doc-ip02-airflow
latency    : feature 5.8ms | retrieval 52.4ms | llm 3954.0ms | total 4206.6ms
```

**Hai bằng chứng bền vững vẫn còn kiểm tra được sau khi tunnel chết:**

1. MLflow `lab28-rag-release` **v6** giữ alias `champion`, ghi
   `vllm_model_id = Qwen/Qwen3-4B-Instruct-2507` — chạy `uv run lab28 inspect` để xem.
2. Trace `463242b83527480aa428478ce64a6c04` trong Jaeger có span
   **`lab28.vllm.chat_completion`**. Span đó chỉ tồn tại khi client thực sự gọi được
   endpoint suy luận.

`pytest integration-tests -m gpu` chạy khi endpoint còn sống: **9 passed**. Sáu test còn
lại (4 error trace-coverage, `test_the_gateway_stops_routing_to_a_pod_that_is_not_ready`,
`test_the_inference_endpoint_is_scraped`) rơi vào cuối lần chạy dài 1 giờ 51 phút, đúng
lúc session Kaggle hết hạn. Muốn đóng nốt thì mở lại tunnel rồi chạy lại nhóm test đó.

Ghi chú độ trễ: `llm 3954ms` vượt xa budget 500ms. Nguyên nhân là model đi qua tunnel
công cộng từ Kaggle, không phải lỗi hệ thống — nhưng đúng là lý do vì sao endpoint suy
luận ở xa không dùng làm baseline SLO được.

### Vì sao không chạy được vLLM ngay trên máy này

Máy có GPU thật (RTX 3060 Laptop, 6 GiB) và Docker thấy được GPU, nhưng container vLLM
0.28 chết khi khởi tạo engine:

```text
RuntimeError: UVA is not available   (vllm/v1/worker/gpu/buffer_utils.py:47)
```

Nguyên nhân gốc, kiểm chứng bằng chính image vLLM:

| Bước | Quan sát |
|---|---|
| Kernel trong container | `6.6.87.2-microsoft-standard-WSL2` |
| `in_wsl()` | `True` (uname chứa `microsoft`) |
| `is_pin_memory_available()` | `False` — vLLM chủ động tắt theo giới hạn NVIDIA cho CUDA trên WSL |
| `is_uva_available()` | `False` |
| `UvaBuffer.__init__` | raise, vì 0.28 **bắt buộc** UVA |

Không phải lỗi VRAM: `torch.empty(..., pin_memory=True)` vẫn chạy được trong container.
Docker Desktop trên Windows chạy container trong máy ảo WSL2, nên cần kernel khác — đó
là lý do dùng Kaggle. Quy trình đầy đủ ở [`ip07-vllm-kaggle.md`](ip07-vllm-kaggle.md).

Trên Kaggle cũng gặp một lỗi nữa đáng ghi: `torchcodec` ném `OSError` vì thiếu
`libnvrtc.so.13` (nó build cho CUDA 13, môi trường chạy CUDA 12), trong khi vLLM chỉ bọc
import đó bằng `except (ImportError, RuntimeError)` — không bắt `OSError`. Gỡ
`torchcodec` là xong, vì khi đó import ném `ModuleNotFoundError` và guard bắt được.

## IP10 — trace liên tục

Trạng thái: **PASS — 11/11 span bắt buộc** trên một trace duy nhất.

```text
trace_id  : 463242b83527480aa428478ce64a6c04
span_count: 34
```

Đủ cả 11 span mà `contracts/integration-matrix.yaml` yêu cầu:

`lab28.gateway.request`, `lab28.api.ingest`, `lab28.kafka.produce`,
`lab28.kafka.consume`, `lab28.airflow.dag`, `lab28.spark.delta_merge`,
`lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`,
`lab28.mlflow.resolve_release`, `lab28.vllm.chat_completion`.

Một trace đi liền từ gateway, qua API, Kafka, Airflow, Spark, Feast, Qdrant, MLflow tới
vLLM. Trước khi nối được vLLM thì trace chỉ đạt 6/11, vì năm span còn lại đều nằm trên
đường `/ask` — đúng như thiết kế ở `pipeline.py`: inference là dependency **duy nhất
không có degraded path**.

Nhánh LangSmith của IP10: **UNVERIFIED** — không có `LANGSMITH_API_KEY`. Đây là gate
theo môi trường mà `contracts/integration-matrix.yaml` đã khai báo sẵn.

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

## Tài liệu đi kèm

| Tài liệu | Nội dung |
|---|---|
| [`../ANSWERS.md`](../ANSWERS.md) | Trade-off, 20 production gap, đóng góp, Q&A |
| [`incident-record.md`](incident-record.md) | Biên bản sự cố Feast + chứng minh không mất/không trùng dữ liệu |
| [`performance-profile.md`](performance-profile.md) | P50/P95/P99 ở 8 và 16 worker + phân tích nút thắt |
| [`ip07-vllm-kaggle.md`](ip07-vllm-kaggle.md) | Vì sao vLLM trượt trên WSL2 và cách lấy evidence IP07 qua Kaggle |
| [`demo-runbook.md`](demo-runbook.md) | Thứ tự trình bày |

## Kiểm tra lại sau sự cố

Toàn bộ battery được chạy **lại sau** khi inject sự cố Feast và khôi phục, tất cả vẫn đạt —
bản thân điều này là bằng chứng recovery:

```text
uv run ruff check .                                        -> All checks passed
uv run pytest starter-tests tests -q                       -> 87 passed
uv run python scripts/verify_matrix.py                     -> 245 checks passed
uv run python scripts/check_portability.py                 -> OK
uv run python scripts/validate_manifests.py                -> passed
uv run pytest integration-tests -m "not gpu and not langsmith" -q -> 56 passed, 16 deselected
```

## Lệnh sinh evidence file

```text
uv run lab28 evidence
uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

`.gitignore` loại trừ thư mục `evidence/` (nó là output runtime), nên các file JSON này
được nộp kèm riêng chứ không commit vào repo.
