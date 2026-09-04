# Kịch bản demo — Day 28 Track 2

- **Người trình bày:** Nguyễn Thế Khiêm — 2A202601036
- **Thời lượng mục tiêu:** 18–20 phút trình bày + Q&A
- Bám theo thứ tự của [`demo-runbook.md`](demo-runbook.md). Câu hỏi dự kiến nằm ở
  [`qa-bank.md`](qa-bank.md).

> Số liệu trong tài liệu này là số **thật** ở lần chạy gần nhất. Chúng sẽ đổi mỗi lần
> chạy lại. **Đừng học thuộc số** — chạy lệnh và đọc số hiện trên màn hình. Người chấm
> quan tâm bạn giải thích được con số, không phải bạn nhớ được nó.

---

## T-30 phút — chuẩn bị

```powershell
$env:PYTHONUTF8 = "1"          # bắt buộc trên Windows, xem mục "Bẫy" bên dưới

docker compose --env-file ports.template --profile full up -d --wait
docker compose --env-file ports.template --profile full ps
```

**Làm nóng consumer group** — quan trọng, đừng bỏ:

```powershell
uv run lab28 seed --via-gateway
# rồi trigger DAG một lần và đợi nó success
```

Lý do ở mục "Bẫy" số 2. Chạy trước một lần thì lúc demo DAG mới đọc được dữ liệu ngay.

**Mở sẵn 6 tab trình duyệt:**

| Tab | URL | Dùng ở phần |
|---|---|---|
| Airflow | http://localhost:8082 | 2 |
| Jaeger | http://localhost:16686 | 3 |
| Grafana | http://localhost:3000 | 4 |
| Prometheus | http://localhost:9090/targets | 4 |
| MLflow | http://localhost:5000 | 6 |
| Qdrant | http://localhost:6333/dashboard | 2 |

**Kiểm tra lần cuối:**

```powershell
uv run lab28 inspect
```

---

## Phần 1 — Kiến trúc và quyền sở hữu (2 phút)

Mở ảnh `docs/images/lab28-architecture-overview.png`.

**Nói:**

> Hệ thống có 5 tầng và 10 ranh giới tích hợp. Đọc theo ba vùng màu: luồng chính đi từ
> người dùng qua Envoy, FastAPI, Kafka, Airflow rồi ghi vào Delta Lake. Vùng thứ hai là
> dữ liệu và mô hình: Delta cấp cho Feast, Qdrant và MLflow. Vùng thứ ba là giám sát.
>
> Bài này em làm cá nhân nên em đảm nhiệm cả 5 vai. Nguồn sự thật cho 10 điểm tích hợp
> là `contracts/integration-matrix.yaml` — mỗi điểm ghi rõ contract vào/ra, tín hiệu
> sức khỏe, test nào phủ, và file evidence nào phải có.

**Chỉ vào file, đừng chỉ nói:**

```powershell
uv run python scripts/verify_matrix.py
```

> 245 phép kiểm tra xác nhận ma trận này khớp với repo: không có test nào được khai báo
> mà không tồn tại, không có IP nào thiếu evidence.

---

## Phần 2 — Happy path (4 phút)

**Nói trước khi chạy:** "Em sẽ gửi dữ liệu qua gateway, rồi đi theo đúng một bản ghi
xuyên 6 thành phần."

```powershell
uv run lab28 seed --via-gateway
```

Dừng lại ở output, chỉ vào **một** bản ghi:

> Mỗi phản hồi trả `202 Accepted`, không phải `201 Created`. Đây là chủ ý: API mới chỉ
> giao event cho Kafka; Delta, Feast, Qdrant được cập nhật sau bởi Airflow. Nói `201` là
> nói về việc chưa xảy ra. Cái em sẽ bám theo là `idempotency_key` này.

```powershell
uv run lab28 inspect
```

> Delta `feedback` đang ở version 17 với 23 dòng, `documents` version 11 với 20 dòng.
> `last_operation` là `MERGE`. Qdrant có 20 điểm. MLflow có `lab28-rag-release` v6 giữ
> alias `champion`, và nó ghi đúng `vllm_model_id` của endpoint đang phục vụ.

Mở tab **Airflow** → DAG `lab28_ingestion_pipeline` → chọn run mới nhất → 4 task xanh.

Mở log task `drain_kafka_into_delta`, tìm dòng:

```text
polled: 86, processed: 22, dead_lettered: 0
```

**Đây là điểm nhấn mạnh nhất của bài. Nói chậm:**

> 86 bản tin thô, nhưng chỉ 22 bản ghi được ghi xuống. Đây là hàm `dedupe_latest` em
> viết. Nó phải chạy **trước** khi vào Delta, vì `MERGE` của Delta sẽ **ném lỗi** nếu
> source có hai dòng cùng khớp một dòng đích. Nghĩa là replay không chỉ tạo dữ liệu
> trùng — nó làm hỏng cả job.

---

## Phần 3 — Trace xuyên hệ thống (2 phút)

```powershell
type evidence\ip10-trace.json
```

Lấy `trace_id` (lần chạy gần nhất: `463242b83527480aa428478ce64a6c04`), dán vào **Jaeger**.

> Một trace ID duy nhất, 34 span, đi liền từ gateway qua API, Kafka producer, Kafka
> consumer, Airflow DAG, Spark MERGE, Feast, Qdrant, MLflow tới vLLM. Đủ **11 trên 11**
> span mà ma trận yêu cầu.
>
> Điểm đáng nói: trước khi nối được vLLM thật, trace này chỉ đạt 6/11. Năm span còn lại
> đều nằm trên đường `/ask`, và đường đó không có degraded path — không có model thì
> không có câu trả lời, nên cũng không có span.

---

## Phần 4 — Golden signals (2 phút)

Mở **Prometheus → Targets**.

> 9 trên 10 target đang `up`. Cái `down` là `lab28-vllm-optional`, vì endpoint suy luận
> nằm trên Kaggle và tunnel là tạm thời. Tên job có chữ `optional` là cố ý.
>
> Em có sửa một chỗ ở đây: `prometheus.yml` gốc ghim cứng địa chỉ local
> `host.docker.internal:8001`, nên không scrape được endpoint ở xa mà chính đề bài
> khuyến khích dùng. Em đổi sang `file_sd_configs` đọc `monitoring/targets/vllm.json`,
> trong đó `__scheme__` cho phép đặt `http` hay `https` theo từng target.

Mở **Grafana** → dashboard `platform-overview`.

> Bốn golden signal: rate, errors, duration, saturation, cộng Kafka consumer lag.

Nhắc alert:

```powershell
type evidence\ip09-prometheus-targets.json
```

> Hai alert rule đã nạp và đang được đánh giá: `Lab28ApiUnavailable` và
> `Lab28HighErrorRatio`.

---

## Phần 5 — Sự cố và khôi phục (4 phút) — **phần ăn điểm nhất**

Đọc trước [`incident-record.md`](incident-record.md). Trình tự bắt buộc là
**dự đoán → inject → quan sát → khôi phục → chứng minh**.

**Bước 1 — dự đoán TRƯỚC khi gõ lệnh.** Nói ra 5 điều:

> Em sẽ dừng Feast. Em dự đoán: một, `/ready` chuyển sang `degraded` chứ không phải
> `not_ready`, vì `probe_feast` khai báo `mandatory=False`. Hai, ingestion vẫn nhận
> được dữ liệu vì đường HTTP đến Kafka không đi qua Feast. Ba, DAG sẽ hỏng đúng ở task
> `refresh_online_features`. Bốn, Delta không mất bản ghi nào. Năm, gửi lại lô cũ thì
> Delta tăng version nhưng không tăng số dòng.

**Bước 2 — inject:**

```powershell
docker compose --env-file ports.template --profile full stop feast
curl.exe http://localhost:8000/ready
```

> `status = degraded`. `feast.ready = false`, nhưng `kafka`, `mlflow`, `qdrant` vẫn
> `true`. Đây là hàm `readiness_status` em viết.
>
> Vì sao ba trạng thái chứ không phải hai? Vì `/ready` quyết định gateway có rút pod ra
> khỏi vòng quay hay không. Nếu em gộp `degraded` vào `not_ready`, thì một Feast lạnh sẽ
> rút **toàn bộ** pod khỏi tải, trong khi hệ thống vẫn trả lời đúng bằng feature mặc
> định. Em sẽ tự gây ra outage.

**Bước 3 — chứng minh không mất dữ liệu:**

```powershell
uv run lab28 seed --via-gateway
```

> Vẫn `accepted`. Giờ em chạy lại DAG với đúng lô dữ liệu cũ.

Trigger DAG, đợi, rồi:

```powershell
uv run lab28 inspect
```

> Bảng `documents`: version **tăng**, số dòng **không đổi**. Version tăng nghĩa là
> `MERGE` thật sự đã chạy. Số dòng không đổi nghĩa là mọi bản ghi khớp vào dòng cũ.
> Đó là bằng chứng idempotency.

Bất biến mạnh hơn:

```powershell
uv run python -c "from deltalake import DeltaTable; t=DeltaTable('.lab28/delta/documents'); d=t.to_pandas(); print(f'rows={len(d)} distinct_keys={d.idempotency_key.nunique()}')"
```

> Số dòng **bằng đúng** số khóa duy nhất. Nếu replay tạo bản sao thì `rows` sẽ lớn hơn.

**Bước 4 — khôi phục:**

```powershell
docker compose --env-file ports.template --profile full start feast
curl.exe http://localhost:8000/ready
```

> `feast.ready` về `true`.

**Câu chốt của phần này — nói ra, nó ghi điểm:**

> Một điều đáng nói: DAG báo `failed` ở đây là **hành vi đúng**, không phải lỗi cần
> giấu. Nó cho biết online store đang tụt lại so với lakehouse. Nếu task đó nuốt lỗi và
> báo `success`, Feast sẽ âm thầm phục vụ feature cũ — và đó mới là hỏng thật.

---

## Phần 6 — Promotion và rollback (2 phút)

```powershell
uv run lab28 inspect     # champion hiện tại
uv run lab28 rollback
uv run lab28 inspect     # champion đã lùi một version
```

> `champion` là một **alias**, không phải số version ghim trong code. Rollback vì vậy
> là một lệnh đổi alias: không sửa code, không build lại image, không deploy lại.
> Serving path đọc alias nên nhận thay đổi ở lần refresh kế tiếp.

Mở **MLflow UI** để chỉ alias đã dịch chuyển.

Trả lại trạng thái:

```powershell
uv run lab28 release
```

**Trung thực khi bị hỏi sâu** (xem Q&A câu 9): topic `model.events` và contract
`ModelLifecycleEvent` đã khai báo nhưng **chưa có thành phần nào publish**. Bằng chứng
rollback là alias trong MLflow, không phải message trên Kafka. Em đã ghi vào bảng
production gap dòng 19.

---

## Phần 7 — Kubernetes và GitOps (2 phút)

```powershell
uv run python scripts/validate_manifests.py
kubectl kustomize deploy/kubernetes/base | Select-String "^kind:"
```

> 10 resource render sạch: Namespace, ServiceAccount, ConfigMap, Deployment, Service,
> HPA, PodDisruptionBudget, NetworkPolicy, Gateway, HTTPRoute.

Mở `gitops/application.yaml`:

> Rollback theo GitOps là đưa `targetRevision` về tag trước rồi để Argo CD sync.
> `selfHeal: true` nghĩa là sửa tay trên cluster sẽ bị kéo về đúng trạng thái khai báo.
>
> Em **chưa** chạy trên cluster thật nên phần drift/self-heal em báo `UNVERIFIED`, chỉ
> xác minh tĩnh. Em không nói là đã chạy khi chưa chạy.

---

## Phần 8 — Kết luận readiness + IP07 (2 phút)

```powershell
uv run lab28 ready
```

Nói thẳng, đừng vòng vo:

> IP07 hiện `UNVERIFIED`. Máy em **có** GPU RTX 3060 và Docker **thấy** được GPU, nhưng
> vLLM không chạy được. Em đã truy đến nguyên nhân gốc:
>
> Docker Desktop trên Windows chạy container trong máy ảo WSL2. vLLM phát hiện WSL rồi
> **chủ động tắt pinned memory** theo giới hạn NVIDIA đã ghi cho CUDA trên WSL. Nhưng
> `UvaBuffer` của vLLM 0.28 lại **bắt buộc** phải có UVA, nên nó raise
> `RuntimeError: UVA is not available`.
>
> Em kiểm chứng bằng chính image vLLM: `is_pin_memory_available()` và
> `is_uva_available()` đều `False`, trong khi `torch.empty(pin_memory=True)` vẫn chạy
> được. Nghĩa là không phải lỗi VRAM — giảm model hay giảm `gpu-memory-utilization`
> không cứu được.
>
> Em **không** dựng server giả OpenAI để lấp chỗ, vì gate `probe_identity` đòi đồng thời
> `/version` của chính vLLM và metric `vllm:`, và vì rubric cho 0 điểm nếu làm giả.
> Cách đúng là chạy vLLM ở nơi kernel không phải WSL — em đã viết sẵn quy trình Kaggle ở
> `docs/ip07-vllm-kaggle.md`.

Chốt lại:

> Tổng kết: 8 trên 10 điểm tích hợp PASS với evidence sống, IP10 đạt 6/11 span, IP07
> chặn bởi một giới hạn nền tảng mà em đã chẩn đoán đến tận dòng code.

---

## Bẫy đã gặp thật — nói ra nếu có thời gian, rất ghi điểm

**1. `UnicodeEncodeError` khi chạy `lab28 release` trên Windows.** MLflow in emoji ra
stdout, console Windows dùng codepage `cp1252` nên không encode được và lệnh chết **sau
khi** run đã tạo. Fix: `$env:PYTHONUTF8 = "1"`.

**2. DAG báo `success` nhưng `polled: 0` ở lần chạy đầu.** Consumer group mới cần thời
gian join và nhận partition, trong khi `poll_batch` chỉ chờ `idle_polls=3 × 1 giây`.
Hết 3 giây trước khi được gán partition nên đọc được 0 bản tin. Lần trigger thứ hai đọc
đủ 86. Đây là lý do phải làm nóng consumer group trước khi demo.

---

## Nếu có gì hỏng giữa demo

| Triệu chứng | Xử lý tại chỗ | Nói gì |
|---|---|---|
| DAG `polled: 0` | trigger lại lần nữa | "Đây đúng là bẫy rebalance em vừa nói" — biến sự cố thành điểm cộng |
| `lab28 release` chết vì Unicode | `$env:PYTHONUTF8="1"` rồi chạy lại | giải thích cp1252 |
| Container `unhealthy` | `docker compose logs <ten>` | đọc lỗi **đầu tiên**, đừng đọc lỗi cuối |
| `/ready` = `not_ready` | `uv run lab28 ready` xem probe nào đỏ | chỉ đúng dependency, không đoán |
| Hết thời gian | mở thẳng `evidence/` | 12 file JSON đều có timestamp và ID thật |

**Tuyệt đối không** chạy `lab28 reset --yes` trong lúc demo — nó xóa state và làm mất
luôn bằng chứng trước sự cố.

---

## Checklist ngay trước khi trình bày

- [ ] `$env:PYTHONUTF8 = "1"` đã đặt
- [ ] 14 container `running`, các service có healthcheck đều `healthy`
- [ ] Đã chạy nóng consumer group (DAG success một lần)
- [ ] 6 tab trình duyệt đã mở và đã đăng nhập
- [ ] `evidence/` có đủ 12 file
- [ ] Đã đọc [`qa-bank.md`](qa-bank.md) một lượt
- [ ] Biết nói câu "em chưa chạy phần đó nên em báo UNVERIFIED" mà không lúng túng
