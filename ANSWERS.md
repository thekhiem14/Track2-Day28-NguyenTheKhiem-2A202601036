# ANSWERS — Day 28 Track 2

- **Học viên:** Nguyễn Thế Khiêm — 2A202601036
- **Hình thức:** cá nhân (một người đảm nhiệm cả 5 vai trong [`docs/team-role-cards.md`](docs/team-role-cards.md))
- **Phạm vi tự làm:** 4 hàm trong
  [`src/lab28_platform/integration_tasks.py`](src/lab28_platform/integration_tasks.py),
  chạy và xác minh stack, thu thập evidence, viết phân tích dưới đây.

Tài liệu này trả lời ba yêu cầu của [`SUBMISSION.md`](SUBMISSION.md) mục 8:
**trade-off**, **production gap** và **đóng góp**. Trạng thái chạy thật được ghi ở
[`docs/evidence-index.md`](docs/evidence-index.md); phần nào chưa chứng minh được thì
ghi `UNVERIFIED`, không suy đoán và không làm giả.

---

## 1. Bốn hàm đã hoàn thiện và lý do chọn cách làm

Bốn hàm này nằm đúng trên đường chạy thật: Kafka producer, Delta MERGE, Feast client
và `/ready` gọi trực tiếp chúng, nên mỗi quyết định ở đây là một quyết định vận hành.

### A. `event_headers` — IP01 + IP10

```python
if traceparent:
    headers.append((TRACEPARENT_HEADER, traceparent.encode("utf-8")))
headers.append((IDEMPOTENCY_HEADER, idempotency_key.encode("utf-8")))
```

- **Vì sao bỏ hẳn header khi không có trace, thay vì gửi chuỗi rỗng:** `traceparent`
  rỗng không phải là "không có trace", nó là một W3C header **sai định dạng**.
  Consumer sẽ cố parse và nhận một context hỏng, làm đứt trace ở đúng ranh giới mà
  IP10 cần chứng minh. Không có header nghĩa là "bắt đầu một trace mới ở đây" — đó là
  trạng thái hợp lệ cho CLI producer và cho replay.
- **Vì sao `idempotency-key` luôn có:** DLQ và replay cần đọc được khóa chống trùng
  mà **không phải deserialize payload**. Nếu khóa chỉ nằm trong body, một message
  hỏng body sẽ mất luôn khả năng dedupe khi replay.
- **Vì sao encode UTF-8 tường minh:** `confluent-kafka` yêu cầu header value là bytes;
  encode ở một chỗ duy nhất tránh mỗi call site tự đoán encoding.
- Tên header đặt thành hằng số để producer và consumer không lệch nhau vì lỗi gõ chữ.

### B. `dedupe_latest` — IP03

```python
for event in events:                      # đúng một lượt
    winner = latest.get(event.idempotency_key)
    if winner is None or _merge_rank(event) > _merge_rank(winner):
        latest[event.idempotency_key] = event
return [latest[key] for key in sorted(latest)]
```

- **Vì sao phải dedupe trước khi vào Delta:** `MERGE` của Delta **ném lỗi** khi source
  có hai dòng cùng khớp một dòng target (khóa merge là `idempotency_key`). Nghĩa là
  batch replay không chỉ tạo dữ liệu trùng — nó làm **hỏng cả job**. Dedupe ở tầng
  Python khiến quy tắc này kiểm thử được mà không cần JVM
  (`tests/test_delta_merge_idempotency.py`).
- **Vì sao so `(occurred_at, event_id)` chứ không chỉ `occurred_at`:** hai event cùng
  timestamp là chuyện bình thường (seed script, batch submit). Nếu chỉ so timestamp,
  kết quả phụ thuộc thứ tự Kafka trả về partition — cùng một dữ liệu, hai lần chạy,
  hai kết quả. Thêm `event_id` làm thứ tự trở thành **toàn phần**, nên kết quả chỉ
  phụ thuộc nội dung batch.
- **Vì sao `sorted(latest)`:** đầu ra giống hệt nhau giữa các lần replay, nên khi demo
  có thể `diff` hai lần chạy để chứng minh idempotency thay vì chỉ nói miệng.
- **Vì sao đúng một lượt duyệt:** tham số có thể là generator đang rút một batch
  Kafka; duyệt lần hai sẽ nhận iterator rỗng.
- **Đánh đổi:** giữ cả batch trong RAM là O(số khóa). Chấp nhận được vì batch bị chặn
  bởi cấu hình poll; nếu batch lớn hơn RAM thì phải đẩy dedupe xuống Spark
  (`row_number() over (partition by key order by ...)`), đổi lại mất khả năng kiểm
  thử nhanh không cần JVM.

### C. `feast_online_request` — IP04

- **Vì sao dùng `list(FEATURE_REFS)` chứ không viết lại danh sách:**
  `contracts.FEATURE_REFS` đang được dùng bởi cả registry
  ([`feature-repo/definitions.py`](feature-repo/definitions.py)) lẫn parser phản hồi
  (`feature_store._to_lookup`). Viết lại danh sách ở đây tạo ra **nguồn sự thật thứ
  hai**: thêm một feature vào registry mà quên sửa request thì Feast vẫn trả 200 và
  hệ thống im lặng phục vụ thiếu feature — lỗi khó thấy nhất trong cả bài.
- **Vì sao `full_feature_names = False`:** khóa trong phản hồi trùng đúng tên field
  của `contracts.AskerFeatures` (`avg_rating` thay vì `asker_activity_v1__avg_rating`),
  nên parser không cần cắt chuỗi tiền tố — bớt một chỗ có thể vỡ khi đổi tên view.
- **Vì sao `entities` là list một phần tử:** đúng contract batch của Feast; giữ dạng
  list để sau gộp nhiều asker vào một request mà không phải đổi contract.

### D. `readiness_status` — IP07 + IP08

```python
failed = [p for p in results if not p.get("ready", False)]
if any(p.get("mandatory", True) for p in failed):
    return STATUS_NOT_READY
return STATUS_DEGRADED if failed else STATUS_READY
```

- **Vì sao ba trạng thái chứ không phải hai:** `/ready` quyết định gateway có rút pod
  khỏi vòng quay hay không. Nếu gộp `degraded` vào `not_ready`, một Feast lạnh (chưa
  materialize) sẽ **rút toàn bộ pod khỏi tải** dù pipeline vẫn trả lời đúng bằng
  feature mặc định — tự gây outage. Nếu gộp `degraded` vào `ready`, mất luôn tín hiệu
  cảnh báo sớm.
- **Vì sao `mandatory` mặc định `True` và `ready` mặc định `False`:** readiness phải
  **fail closed**. Một probe mới thêm mà quên khai báo severity sẽ bị coi là bắt buộc
  và làm `/ready` đỏ — sai theo hướng an toàn, dễ phát hiện. Mặc định ngược lại tạo ra
  một `/ready` xanh giả.
- **Vì sao `list(probes)`:** `readiness.serving_readiness` truyền vào một generator;
  cần hai lượt duyệt nên phải vật chất hóa trước.

---

## 2. Trade-off của kiến trúc

### 2.1 Ingestion trả `202`, không phải `201`

API chỉ chịu trách nhiệm giao event cho Kafka một cách bền vững. Delta/Feast/Qdrant
được cập nhật sau bởi Airflow và Spark, nên trả `201 Created` là **nói về việc chưa
xảy ra**. Giá phải trả: client không đọc lại được ngay dữ liệu vừa ghi, phải theo
`event_id`. Đổi lại, ingestion không bị chặn bởi độ trễ lakehouse và vẫn sống khi
Airflow chết.

### 2.2 Idempotency key suy ra từ nội dung khi client không gửi

Client không nghĩ đến retry vẫn được bảo vệ. Giá phải trả: hai feedback **cố ý giống
hệt nhau** của cùng một người sẽ bị gộp làm một. Với feedback, mất mát này nhỏ hơn
nhiều so với nhân bản dữ liệu khi mạng chập chờn.

### 2.3 Dedupe ở Python thay vì trong Spark job

Đã phân tích ở mục 1.B: đổi khả năng mở rộng lấy khả năng kiểm thử. Ở quy mô lab đây
là đánh đổi đúng; ở quy mô production thì ngược lại.

### 2.4 Alias `champion` thay vì ghim số version

Serving path gọi `get_model_version_by_alias(champion)`. Rollback vì vậy là **một lệnh
đổi alias**: không sửa code, không rebuild image, không deploy lại. Giá phải trả: thêm
một lần gọi mạng trên đường serving và một cache cần refresh — rẻ hơn nhiều so với
rollback phải đi qua CI.

### 2.5 `require_real=True` cho vLLM

`llm_client.probe_identity` chỉ chấp nhận endpoint chứng minh được **cả hai**:
`/version` của chính vLLM và metric family `vllm:`. Một server OpenAI-compatible bất
kỳ sẽ **trượt** IP07. Giá phải trả: không có GPU thì IP07 fail thật, không có đường
vòng. Đó chính là mục đích — rubric cho 0 điểm phần này nếu làm giả.

### 2.6 LLM là dependency duy nhất không có degraded path

Feast lạnh thì dùng feature mặc định và ghi lý do. Qdrant rỗng thì trả lời không có
nguồn và ghi lý do. Nhưng không có model thì **không có câu trả lời nào để trả**, nên
đó là `503` thật. Bịa một câu trả lời khi LLM chết sẽ phá vỡ chính grounding contract
mà `DEFAULT_SYSTEM_PROMPT` đang bắt buộc.

### 2.7 Vượt latency budget không làm fail request

Budget mặc định `feature 5ms / retrieval 50ms / llm 500ms / total 1000ms` chỉ tăng
`lab28_latency_budget_exceeded_total`. Giết một request chậm không làm người dùng vui
hơn, nhưng làm mất dữ liệu chẩn đoán. Giá phải trả: bắt buộc phải có alert trên metric
đó, nếu không thì regression độ trễ trôi qua âm thầm.

### 2.8 Audit trail lưu hash, không lưu text

`AuditTrail` giữ `input_hash`, `output_hash`, độ dài và token count. Truy được "cùng
một câu hỏi" mà không lưu nội dung người dùng. Giá phải trả: khi debug một câu trả lời
tệ thì không đọc lại được câu hỏi gốc từ log.

### 2.9 Feast/Spark/Airflow không nằm trong `pyproject.toml`

`feast==0.66.0` chặn `prometheus-client<0.25`; đưa nó vào cùng môi trường resolve sẽ
kéo tụt cả serving stack. Mỗi engine ghim version trong image của nó
(`docker/*/requirements.txt`). Giá phải trả: pin nằm ở nhiều nơi, phải nhớ đồng bộ.

### 2.10 Client được **khởi tạo** chứ không **kết nối** lúc start

`Runtime.build` không gọi mạng. Một pod không được phép chết lúc khởi động chỉ vì
MLflow đang restart, vì khi đó nó **không còn `/ready` để nói cho ai biết lý do**. Lỗi
được đẩy sang thời điểm gọi và hiện ra trong readiness breakdown.

---

## 3. Production gaps — còn thiếu gì để chạy thật

| # | Khoảng trống | Rủi ro thật | Hướng xử lý |
|---|---|---|---|
| 1 | Kafka `replication_factor=1`, một broker | Mất broker là mất dữ liệu chưa xử lý | RF≥3, `min.insync.replicas=2`, producer `acks=all` |
| 2 | Không có auth trên Kafka/Qdrant/MLflow/Feast | Vào được network là đọc/ghi được tất cả | mTLS + SASL, RBAC theo service; `NetworkPolicy` hiện có là cần nhưng chưa đủ |
| 3 | Rate limit là **local** trên từng Envoy (`max_tokens: 10`) | Scale ra N pod thì hạn mức thật thành N×10 | Envoy global rate limit service + backend chia sẻ |
| 4 | Không có schema registry | `schema_version` do code tự kiểm; producer sai vẫn publish được | Schema Registry, từ chối ở phía broker |
| 5 | Không có quota/backpressure cho ingestion | Một client lỗi có thể lấp đầy `data.raw` | Quota theo API key, trả `429` sớm ở gateway |
| 6 | DLQ chưa có alert, replay còn thủ công | Message chết nằm im cho tới khi có người nhìn | Alert khi `dead_letter_count > 0`, runbook replay có phê duyệt |
| 7 | Feast materialize theo lịch, không streaming | Freshness trễ bằng chu kỳ DAG | Push source từ `data.processed`, alert `lab28_feature_freshness_seconds` |
| 8 | Embedding model ghim nhưng không có backfill khi đổi | Đổi model là toàn bộ vector cũ sai không gian | Version hóa collection, reindex xong mới chuyển alias |
| 9 | Không có canary/shadow khi promote | `champion` đổi là 100% traffic đổi theo | Traffic split ở gateway, so sánh metric online trước khi chuyển hẳn |
| 10 | Không có eval gate tự động trước promote | Model tệ vẫn promote được nếu người chạy lệnh muốn | Bắt buộc ngưỡng eval trong bước release, chặn promote nếu không đạt |
| 11 | Trace lấy mẫu 100% | Chi phí và dung lượng không chịu nổi ở tải thật | Tail-based sampling: giữ 100% lỗi/chậm, giảm phần còn lại |
| 12 | Secret nằm trong env của Compose | Không rotate được, lộ qua `docker inspect` | External Secrets/Vault, rotate định kỳ |
| 13 | Không có backup/restore cho Delta và MLflow | Mất volume là mất cả lineage lẫn registry | Snapshot định kỳ + diễn tập restore, `VACUUM` có giữ retention |
| 14 | Chưa có SLO/error budget chính thức | Có alert nhưng không gắn với cam kết nào | SLO cho `/ask` p95 và tỷ lệ lỗi, alert theo burn rate |
| 15 | Có probe và `runAsNonRoot` nhưng chưa có image scan/ký ảnh | Image dính lỗ hổng vẫn deploy được | Quét trong CI, PodSecurity `restricted`, ký image |
| 16 | Argo CD `selfHeal` + `prune` không có sync window | Sync ngoài ý muốn giữa giờ cao điểm | Sync window, phê duyệt cho môi trường prod |
| 17 | GPU là điểm chết đơn lẻ | vLLM chết là `/ask` chết | Nhiều replica + hàng đợi, hoặc model dự phòng nhỏ hơn |
| 18 | Chưa có soak test | Rò rỉ bộ nhớ chỉ lộ sau nhiều giờ | Chạy tải dài và theo dõi RSS theo thời gian |
| 19 | Topic `model.events` khai báo nhưng **không có producer** | Không có audit trail dạng sự kiện cho promote/rollback; consumer nào subscribe sẽ chờ mãi | `ReleaseRegistry.promote/rollback` publish `ModelLifecycleEvent` (contract và publisher đã sẵn sàng), hoặc bỏ topic nếu không dùng |
| 20 | `/ready` gọi 5 probe tuần tự, không cache | Một dependency chết kéo chậm readiness của cả pod; xem `docs/performance-profile.md` | Cache theo TTL ngắn, chạy probe song song, timeout riêng cho từng probe |

---

## 4. Đóng góp

Bài làm **cá nhân**. Một người thực hiện tuần tự cả năm vai:

| Vai | Việc đã làm |
|---|---|
| Ingestion & Orchestration (IP01–IP02) | `event_headers`; kiểm tra topic/retention; đọc đường DLQ–replay trong `event_bus`; chạy DAG `lab28_ingestion_pipeline` |
| Data & ML (IP03–IP04–IP06) | `dedupe_latest`, `feast_online_request`; kiểm chứng Delta history và time travel; `lab28 release` và rollback theo alias |
| Serving & Retrieval (IP05–IP07) | Kiểm chứng point ID tất định; `lab28 index`; xác minh danh tính vLLM bằng `probe_identity` |
| Platform & Observability (IP08–IP10) | `readiness_status`; xác minh gateway 200/429; Prometheus targets; đối chiếu span theo `required_spans` |
| Presenter / Incident Commander | Thứ tự demo theo [`docs/demo-runbook.md`](docs/demo-runbook.md), chỉ mục evidence, kịch bản sự cố ở mục 5 |

---

## 5. Kịch bản sự cố dùng khi demo

**Dự đoán trước khi inject** — dừng container Feast:

1. `lab28_feature_lookup_seconds{outcome="unavailable"}` tăng;
2. `/ready` chuyển `degraded` (Feast là probe **không** bắt buộc), **không** phải `not_ready`;
3. `/ask` vẫn trả `200`, `evidence.degraded=true` và `degraded_reasons` nêu feature store;
4. `lab28_degraded_responses_total` tăng;
5. Kafka/Delta **không** mất bản ghi nào — ingestion không đi qua Feast.

**Khôi phục:** bật lại Feast, materialize, `/ready` trở về `ready`.

**Chứng minh không mất dữ liệu:** so số dòng Delta trước và sau sự cố, rồi gửi lại
đúng batch cũ và cho thấy Delta version tăng nhưng **số dòng không đổi** — đó là
`dedupe_latest` đang làm việc.

Chọn Feast vì nó phân biệt rõ nhất `degraded` với `not_ready` — đúng câu hỏi mà
`readiness_status` phải trả lời.

---

## 6. Trả lời nhanh các câu hỏi Q&A hay gặp

**`ready` / `degraded` / `not_ready` khác nhau thế nào?**
`ready`: mọi probe xanh. `degraded`: chỉ probe **không bắt buộc** đỏ — pod vẫn nhận
tải, câu trả lời tự khai báo là suy giảm. `not_ready`: có probe **bắt buộc** đỏ — pod
phải bị rút khỏi vòng quay. `/health` không bao giờ chạm dependency, nếu không một
Kafka chậm sẽ làm Kubernetes restart pod và hỏng thêm.

**Vì sao replay không tạo dữ liệu trùng?**
Bốn tầng dùng chung một khóa logic: key của Kafka message là `idempotency_key`; Delta
`MERGE` trên đúng khóa đó sau khi `dedupe_latest` bảo đảm source duy nhất; Qdrant point
ID là `uuid5(ID_NAMESPACE, doc_id)` nên upsert đè đúng một điểm; Feast ghi theo entity.

**Vì sao không thể "giả" IP07?**
`probe_identity` đòi `/version` của vLLM **và** metric `vllm:`. Một proxy
OpenAI-compatible không sinh được cả hai.

**Rollback model không sửa code bằng cách nào?**
`champion` là alias. `model_registry.rollback()` tìm version cao nhất **thấp hơn** version
hiện tại rồi chuyển alias sang đó. Serving path đọc alias nên nhận thay đổi ở lần refresh
kế tiếp — không sửa code, không build lại image.

Đã chạy thật:

```text
uv run lab28 inspect   -> lab28-rag-release v2 is champion
uv run lab28 rollback  -> champion moved from v2 to v1
uv run lab28 inspect   -> lab28-rag-release v1 is champion
```

Hai điều cần nói đúng nếu bị hỏi sâu:

1. Contract `ModelLifecycleEvent` và topic `model.events` **đã được khai báo**, và
   `event_bus` chấp nhận kiểu này, nhưng **chưa có thành phần nào thực sự publish** sự
   kiện đó. Đã kiểm chứng bằng cách đọc `model.events` sau khi rollback: topic rỗng.
   Xem mục 3, dòng 19.
2. `promote()` có tăng `lab28_release_transitions_total{action=...}`, nhưng counter đó
   nằm **trong tiến trình thực hiện lệnh**. Chạy `lab28 rollback` từ host là một tiến
   trình CLI sống vài giây, Prometheus không kịp scrape — nên `/metrics` của container
   API vẫn trống. Bằng chứng bền vững của rollback là **alias trong MLflow**, không phải
   counter này.
