# Incident record — Feast down, degraded, recovery, no data loss

Chạy thật theo [`runbooks/failure-injection.md`](../runbooks/failure-injection.md),
kịch bản "Feast down". Chọn Feast vì nó là cách rõ nhất để phân biệt `degraded` với
`not_ready` — đúng câu hỏi mà `readiness_status` phải trả lời.

Không dùng `down -v` ở bất kỳ bước nào, nên state trước sự cố được giữ nguyên.

## Dự đoán trước khi inject

Ghi trước để đối chiếu, không sửa sau:

1. `/ready` chuyển sang `degraded` chứ **không** `not_ready`, vì `probe_feast` khai báo
   `mandatory=False`;
2. ingestion vẫn nhận được dữ liệu, vì đường HTTP → Kafka **không** đi qua Feast;
3. DAG sẽ hỏng ở đúng task `refresh_online_features`, các task khác vẫn xanh;
4. Delta không mất bản ghi nào;
5. gửi lại đúng lô cũ thì Delta **tăng version nhưng không tăng số dòng**.

## Diễn biến

| Thời điểm (UTC) | Hành động | Quan sát |
|---|---|---|
| trước | trạng thái nền | `feedback` v10 / 15 rows; `documents` v7 / 18 rows; `/ready` = `degraded`, `feast.ready = true` |
| 17:36:28 | `docker compose stop feast` | container dừng |
| +3s | `GET /ready` | `status = degraded`; `feast.ready = false` (`unreachable: ConnectError`); `kafka`, `mlflow`, `qdrant` vẫn `true` |
| trong sự cố | `lab28 seed --via-gateway` | **13 document + 9 feedback vẫn `accepted`** |
| trong sự cố | trigger DAG `incident-c8836bb0` | `drain_kafka_into_delta` **success**, `index_new_documents` **success**, `refresh_online_features` **failed**, `announce_processed_batch` `upstream_failed` |
| sau replay | đọc Delta | `feedback` v11 / 18 rows; `documents` v8 / **18 rows** |
| 17:39:39 | `docker compose start feast` | container khỏe lại |
| sau recovery | `GET /ready` | `feast.ready = true`, `status = degraded` (chỉ còn vLLM đỏ, xem IP07) |

Cả 5 dự đoán đều đúng.

## Chứng minh không mất và không trùng dữ liệu

**Bằng chứng 1 — replay không nhân bản.** Bảng `documents` trước replay có 18 dòng.
Sau khi gửi lại đúng 13 document cũ và chạy lại DAG, bảng lên **version 8** nhưng vẫn
**18 dòng**. Version tăng nghĩa là `MERGE` thật sự đã chạy; số dòng không đổi nghĩa là
mọi bản ghi đều khớp vào dòng cũ thay vì tạo dòng mới. Đây chính là `dedupe_latest` cộng
với `MERGE ON target.idempotency_key = source.idempotency_key`.

**Bằng chứng 2 — bất biến khóa.** Đọc trực tiếp Delta bằng `deltalake`:

```text
feedback : version=11 rows=18 distinct_keys=18   -> không trùng
documents: version=8  rows=18 distinct_keys=18   -> không trùng
```

Số dòng **bằng đúng** số khóa idempotency duy nhất ở cả hai bảng. Nếu replay tạo bản sao
thì `rows > distinct_keys`.

**Vì sao `feedback` tăng từ 15 lên 18 mà vẫn coi là đúng.** Đây không phải dữ liệu trùng.
Khóa của feedback suy ra từ nội dung (`fb:<asker>:<content_hash>`), nên cùng nội dung sẽ
cho cùng khóa. Ba dòng tăng thêm là ba feedback **thực sự mới**: ở các lần seed trước,
rate limiter của gateway đã chặn một phần feedback bằng `429`, nên chúng chưa từng vào
Kafka. `rows == distinct_keys` ở trên xác nhận không có bản sao nào.

**Bằng chứng 3 — không mất dữ liệu trong lúc Feast chết.** Ingestion vẫn trả `accepted`
và `drain_kafka_into_delta` vẫn `success` trong đúng lần chạy mà `refresh_online_features`
hỏng. Dữ liệu đã nằm bền vững trong Delta; chỉ có bước materialize sang online store là
chưa chạy. Sau khi Feast sống lại, chỉ cần chạy lại DAG là online store bắt kịp — không
phải gửi lại dữ liệu từ đầu.

## Điều rút ra

Việc DAG báo `failed` ở đây là **hành vi đúng**, không phải lỗi cần giấu: nó cho biết
online store đang tụt lại so với lakehouse. Nếu task đó nuốt lỗi và báo `success`, Feast
sẽ âm thầm phục vụ feature cũ, và đó mới là hỏng thật sự.

`degraded` là câu trả lời đúng thay vì `not_ready`, vì pod vẫn phục vụ được: `/ask` chạy
với feature mặc định và tự khai báo `evidence.degraded = true`. Nếu gộp trường hợp này
thành `not_ready`, gateway sẽ rút toàn bộ pod khỏi vòng quay và biến một sự cố bộ phận
thành outage toàn phần.
