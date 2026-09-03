# IP07 — nối vLLM thật từ Kaggle T4

Tài liệu này ghi (1) vì sao **không** chạy được vLLM ngay trên máy Windows này, và
(2) các bước chính xác để lấy evidence IP07 thật bằng Kaggle.

## 1. Vì sao vLLM 0.28 không chạy dưới Docker Desktop trên Windows

Máy làm bài **có** GPU thật và Docker **có** truy cập được GPU:

```text
docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi
  -> NVIDIA GeForce RTX 3060 Laptop GPU, 6144 MiB
```

Nhưng container vLLM chết ngay khi khởi tạo engine:

```text
RuntimeError: UVA is not available
  vllm/v1/worker/gpu/buffer_utils.py, line 47, in UvaBuffer.__init__
```

Truy nguyên đến tận gốc:

```text
uname -a trong container
  -> Linux 6.6.87.2-microsoft-standard-WSL2

vllm/platforms/interface.py
  def in_wsl() -> bool:
      return "microsoft" in " ".join(platform.uname()).lower()

vllm/utils/platform_utils.py
  def is_uva_available() -> bool:
      return is_pin_memory_available() or current_platform.is_cpu()

Kiểm chứng trong chính image vLLM:
  is_pin_memory_available() -> False
  is_uva_available()        -> False
```

Chuỗi nhân quả: Docker Desktop trên Windows chạy container trong máy ảo **WSL2**, nên
kernel có chữ `microsoft`. vLLM nhận ra WSL và **chủ động tắt pinned memory** theo
[giới hạn NVIDIA đã ghi cho CUDA trên WSL](https://docs.nvidia.com/cuda/wsl-user-guide/index.html#known-limitations-for-linux-cuda-applications).
`UvaBuffer` trong vLLM 0.28 lại **bắt buộc** phải có UVA, nên nó raise thay vì chạy chậm hơn.

Kết luận: đây **không** phải lỗi GPU, driver hay cấu hình Compose. Đổi model nhỏ hơn,
giảm `--gpu-memory-utilization` hay thêm `--enforce-eager` đều không giải quyết được, vì
điều kiện thất bại không liên quan tới dung lượng VRAM. Cách xử lý là chạy vLLM ở nơi
kernel **không phải** WSL: Kaggle, máy Linux thật, hoặc cluster của lớp.

Ghi chú phụ: VRAM 6 GiB (màn hình đã chiếm ~1 GiB) cũng là ràng buộc thật, nên kể cả khi
UVA không chặn thì chỉ nên phục vụ model rất nhỏ. `compose.gpu.local.yaml` giữ sẵn cấu
hình đó cho máy Linux có GPU nhỏ.

## 2. Các bước lấy evidence IP07 bằng Kaggle

### Bước 1 — trên Kaggle

Tạo notebook, chọn **Accelerator: GPU T4 x2**, rồi chạy lần lượt:

```python
!nvidia-smi
!pip install -q "vllm==0.26.0"
```

```python
import subprocess
subprocess.Popen(
    "vllm serve Qwen/Qwen3-4B-Instruct-2507 --host 0.0.0.0 --port 8000 "
    "--dtype half --max-model-len 4096 --gpu-memory-utilization 0.85",
    shell=True,
)
```

Đợi tới khi cell dưới trả về JSON có model id (cold start vài phút vì phải tải weights):

```python
!sleep 240; curl -s http://127.0.0.1:8000/v1/models
```

Kiểm tra đúng ba thứ mà gate IP07 đòi:

```python
!curl -s http://127.0.0.1:8000/version
!curl -s http://127.0.0.1:8000/v1/models
!curl -s http://127.0.0.1:8000/metrics | grep -m 5 "^vllm:"
```

Nếu `/version` rỗng hoặc `/metrics` không có dòng nào bắt đầu bằng `vllm:` thì dừng lại —
endpoint đó sẽ không qua được gate.

### Bước 2 — mở tunnel

```python
!wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /usr/local/bin/cloudflared
!chmod +x /usr/local/bin/cloudflared
!cloudflared tunnel --url http://127.0.0.1:8000 --no-autoupdate
```

Cell này in ra một URL dạng `https://<ngẫu-nhiên>.trycloudflare.com`. **Giữ cell chạy** —
đóng cell là mất tunnel.

### Bước 3 — kiểm tra từ máy mình

```powershell
curl.exe https://<url>.trycloudflare.com/version
curl.exe https://<url>.trycloudflare.com/v1/models
```

### Bước 4 — trỏ stack sang endpoint đó

URL tunnel là **tạm thời và công khai**, nên đặt bằng biến môi trường, **không** commit
và **không** ghi vào `ports.template`:

```powershell
$env:LAB28_VLLM_BASE_URL = "https://<url>.trycloudflare.com/v1"
$env:LAB28_VLLM_MODEL_ID = "Qwen/Qwen3-4B-Instruct-2507"
$env:LAB28_VLLM_REQUIRE_REAL = "true"
$env:PYTHONUTF8 = "1"

uv run lab28 release          # ghi lại model id mới vào champion release
uv run lab28 ready            # kỳ vọng: status = ready
uv run lab28 ask --question "Nền tảng này gồm những thành phần nào?"
uv run lab28 evidence         # ghi evidence/ip07-vllm-identity.json
uv run pytest integration-tests -m gpu -q
uv run python scripts/collect_external_evidence.py   # cập nhật ip10-trace.json
```

Container API cũng cần biết endpoint mới nếu muốn `/ask` qua gateway hoạt động:

```powershell
docker compose --env-file ports.template --profile full up -d --no-deps `
  -e LAB28_VLLM_BASE_URL="https://<url>.trycloudflare.com/v1" api
```

### Bước 5 — điều cần thấy

- `evidence/ip07-vllm-identity.json` có `is_real_vllm: true`, `version` là bản vLLM,
  `served_models` chứa đúng model id, và `vllm_metric_names` không rỗng;
- `lab28 ready` trả `ready` thay vì `not_ready`;
- `evidence/ip10-trace.json` đủ **11/11** span, vì một lần `/ask` thành công sẽ bổ sung
  `lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`,
  `lab28.mlflow.resolve_release` và `lab28.vllm.chat_completion`.

## 3. Giới hạn phải nói khi demo

- Session và quota Kaggle có thể hết giữa buổi; tunnel đứt là IP07 trở lại `not_ready`.
- Tunnel public làm tăng độ trễ và là rủi ro bảo mật; không đặt token lên đó.
- Cold start lâu vì phải tải weights mỗi session mới.
- Kaggle chỉ giải quyết inference. Nó **không** chứng minh Kafka, Delta, Feast, MLflow
  hay observability — những phần đó đã chạy ở local và có evidence riêng.
