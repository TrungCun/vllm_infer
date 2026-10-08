# Gateway sleep/wake cho vLLM

Gateway Go chuyển tiếp request và quản lý sleep level 1. Các client giữ nguyên URL `http://localhost:9094/v1`.

## Chạy

Tại thư mục gốc:

```powershell
docker compose up --build -d
```

Chỉ build gateway; vLLM dùng image có sẵn. Build gateway tự chạy `go test -race`. Khi chỉ sửa gateway:

```powershell
docker compose up --build -d --no-deps gateway
```

## Cấu hình

Đặt trong `.env` hoặc PowerShell rồi chạy lại Compose:

```powershell
$env:IDLE_TIMEOUT = "30m"    # Không có request trong 30 phút thì sleep
$env:CONTROL_TIMEOUT = "60s" # Thời gian tối đa chờ sleep/wake
```

`IDLE_TIMEOUT=0` tắt tự sleep. `CONTROL_TIMEOUT` không giới hạn thời gian sinh câu trả lời.

## Cách hoạt động

- Đang thức: chuyển tiếp ngay, tái sử dụng kết nối, hỗ trợ streaming.
- Đang ngủ: các request cùng chờ một lần wake; chỉ chuyển tiếp khi vLLM sẵn sàng.
- Đếm thời gian rảnh từ lúc response/stream cuối kết thúc; không sleep khi còn request.
- `/v1/models` không wake hoặc reset thời gian rảnh. `/healthz` chỉ kiểm tra gateway.
- `/v1/responses` và `/v1/batches` trả `501`; các endpoint điều khiển vLLM không được công khai.

Mọi request suy luận phải đi qua gateway để theo dõi đúng trạng thái.

## Theo dõi và xử lý lỗi

```powershell
docker compose logs -f gateway
```

Client ngắt kết nối hoặc stream bị lỗi sẽ tắt tự sleep đến khi khởi động lại; suy luận vẫn hoạt động. Sleep/wake thất bại sẽ trả `503`. Sau khi xử lý lỗi backend, khởi động lại cả hai:

```powershell
docker compose restart vllm gateway
```

Độ trễ gateway chưa được đo trên hệ thống thực tế. Có `main_test.go` và `benchmark.py` để kiểm tra.
