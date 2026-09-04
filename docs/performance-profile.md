# Load profile và phân tích nút thắt

Kết quả đo thật theo [`runbooks/performance.md`](../runbooks/performance.md).

## Điều kiện đo

| Mục | Giá trị |
|---|---|
| Phần cứng | 20 CPU, 16 GiB RAM; container giới hạn 7.57 GiB (Docker Desktop/WSL2) |
| GPU | NVIDIA RTX 3060 Laptop, 6 GiB VRAM |
| Stack | Compose profile `full`, 14 service |
| Endpoint đo | `GET /ready` qua gateway `:8080` |
| Warm-up | Có — stack đã chạy seed/index/release và toàn bộ suite tích hợp trước khi đo |
| Degraded policy | `LAB28_ALLOW_DEGRADED=true` trong container API |

Không suy ra capacity production từ số liệu laptop này.

## Kết quả

`uv run python load-tests/run_profile.py --requests 200 --workers 8`:

```json
{"requests": 200, "workers": 8,
 "status_counts": {"200": 29, "0": 171},
 "latency_ms": {"p50": 5.67, "p95": 532.09, "p99": 636.63}}
```

Script chuẩn gộp mọi lỗi thành `0`, nên phải đo lại có tách mã trạng thái:

| Concurrency | 200 | 429 | p50 (tất cả) | p95 (tất cả) | p99 (tất cả) | p50 (chỉ 200) | p95 (chỉ 200) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 workers | 69 | 131 | 8.9 ms | 926 ms | 2419 ms | 595 ms | 2401 ms |
| 16 workers | 15 | 185 | 7.0 ms | 963 ms | 1147 ms | 993 ms | 1166 ms |

Tài nguyên trong lúc đo: API `0.35%` CPU / `227 MiB` RAM, gateway `1.41%` CPU /
`23 MiB`, Kafka `0.97%` CPU / `334 MiB`.

## Đọc số liệu

**1. Percentile tổng hợp ở đây gây hiểu nhầm.** p50 chỉ ~7–9 ms không phải vì hệ
thống nhanh, mà vì **phần lớn request bị từ chối rất nhanh**. Một `429` trả về trong
vài mili giây và kéo p50 xuống. Phải tách theo mã trạng thái mới thấy đúng: request
được phục vụ thật có p50 khoảng 0.6–1.0 giây.

**2. Nút thắt thứ nhất: rate limit của gateway, không phải ứng dụng.** Envoy cấu hình
`max_tokens: 10`, `tokens_per_fill: 10`. Tăng từ 8 lên 16 worker làm tỉ lệ `200` **giảm**
từ 35% xuống 7.5% — thêm tải chỉ tạo thêm `429`. Đây là hành vi **đúng thiết kế**
(bảo vệ backend), không phải lỗi hiệu năng.

**3. Nút thắt thứ hai: `/ready` là endpoint đắt.** API chỉ dùng 0.35% CPU và 227 MiB
RAM trong khi p95 tới hơn 2 giây — nghĩa là thời gian trôi ở **chờ mạng**, không phải
ở tính toán. `/ready` gọi tuần tự 5 probe (Kafka, MLflow, Qdrant, vLLM, Feast), và
probe vLLM phải **đợi hết connect timeout** vì lúc đo endpoint chưa bật. Một dependency
chết làm chậm cả readiness của những dependency đang sống.

**4. Vì sao không được dùng số này làm SLO cho `/ask`.** `/ready` cố ý chạm mọi
dependency; `/ask` thì không. Muốn có SLO thật phải đo `/api/v1/ask` với corpus đại
diện và vLLM đang chạy — xem mục dưới.

## Hướng cải thiện, theo thứ tự đáng làm

1. **Cache kết quả probe trong `/ready`** (TTL 1–2 giây) hoặc chạy song song. Hiện tại
   mỗi lần gọi là 5 lần đi mạng tuần tự; dưới tải, chính readiness tự tạo tải.
2. **Đặt timeout ngắn và riêng cho từng probe.** Một endpoint chết không được phép
   quyết định độ trễ của `/ready`.
3. **Rate limit toàn cục thay vì local.** `max_tokens: 10` là hạn mức **trên mỗi
   Envoy**; scale ra N pod thì hạn mức thật thành N×10 — xem mục 3 bảng production gap
   trong [`../ANSWERS.md`](../ANSWERS.md).
4. **Sửa `load-tests/run_profile.py` để giữ mã trạng thái.** Gộp `429` và lỗi kết nối
   thành `0` làm mất đúng thông tin cần cho phân tích. Đây là lý do bảng trên phải đo lại.
5. **Đo `/api/v1/ask`,** vì đó mới là đường người dùng thật đi qua: feature → retrieval
   → LLM.

## Ngân sách độ trễ đang cấu hình

`feature 5 ms`, `retrieval 50 ms`, `llm 500 ms`, `total 1000 ms`. Vượt ngưỡng **không**
làm hỏng request, chỉ tăng `lab28_latency_budget_exceeded_total{component=...}`. Vì vậy
metric đó phải có alert, nếu không thì regression độ trễ trôi qua âm thầm.
