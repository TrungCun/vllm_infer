# Test gateway

Chạy từ PowerShell tại thư mục gốc. Cần Docker Desktop; không cần cài Go/Python.
Nếu PowerShell chặn chạy file script, dùng:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\run.ps1 -Live
```

Tùy chọn này chỉ áp dụng cho tiến trình PowerShell vừa mở.

## Test độc lập, không ảnh hưởng vLLM

```powershell
.\tests\run.ps1
```

Build binary gateway thật, chạy Go race tests rồi test HTTP với backend giả:

- Đường awake, giữ header/body, streaming và request đồng thời.
- Tự sleep level 1, reset idle, nhiều chu kỳ sleep/wake.
- Không sleep giữa stream; request tới khi đang sleep phải chờ.
- Nhiều request cùng chờ một wake; HTTP wake trả sớm vẫn phải chờ readiness.
- Sleep/wake lỗi hoặc timeout, startup lỗi, client hủy stream hoặc hủy lúc chờ wake.
- Chặn endpoint điều khiển/background; discovery không wake/reset idle.
- Benchmark backend giả: 40 request mỗi đường, keep-alive, p50/p95.

## Test hệ thống vLLM đang chạy

Tạm dừng các agent/client khác trước khi chạy:

```powershell
.\tests\run.ps1 -Live

# Tăng mẫu và tải đồng thời
.\tests\run.ps1 -Live -Requests 100 -Concurrency 16 -Cycles 3
```

Script chạy test độc lập trước; tắt sleep khi benchmark gọi trực tiếp backend,
rồi tạm đặt idle gateway thành 10 giây để test sleep/wake.
Build và tạo lại container gateway từ source hiện tại; vLLM giữ nguyên. Khi kết thúc/lỗi, tự khôi phục
timeout gateway ban đầu. Không sửa `.env`. Request đang chạy có thể bị ngắt
khi tạo lại gateway, nên chạy lúc hệ thống rảnh.

Test thật đo:

- TTFT và tổng thời gian p50/p95: gọi trực tiếp vLLM và qua gateway, tải 1 và tải đồng thời.
- Độ trễ `/v1/models` để tham khảo chi phí proxy trên request nhẹ.
- Reset idle, tự sleep, discovery lúc sleep và wake từ nhiều request đồng thời.
- Streaming dài hơn idle, không sleep giữa stream và đếm idle từ EOF.
  Nếu stream kết thúc quá nhanh, mục này ghi `SKIP`.

Kết quả trong `tests/results/<thời-gian>/`: JSON chứa từng mẫu đo, PASS/FAIL/SKIP
và log gateway. Exit code khác 0 khi test lỗi. TTFT đo từ lúc gửi request đến
token nội dung đầu tiên; luôn đọc stream đến EOF.

Chênh p50/p95 chứa cả nhiễu mạng, lịch chạy và model; không phải overhead riêng
của từng request. Không áp ngưỡng ms cứng. Số lần wake chính xác và lỗi giả lập
được kiểm tra ở suite độc lập, không suy ra từ TTFT.

Nếu lần test trước làm gateway rơi vào trạng thái lỗi hoặc tắt auto-sleep,
xử lý nguyên nhân rồi chạy `docker compose restart vllm gateway` trước khi test lại.
Nếu terminal bị đóng cưỡng bức thì khối khôi phục có thể không chạy; áp lại
cấu hình `.env` bằng `docker compose up -d --no-deps --force-recreate gateway`.

Khi phải chờ wake/readiness, gateway đọc body trước để phát hiện client hủy request.
Body trên nhánh này giới hạn 32 MiB (vượt giới hạn trả `413`), thời gian đọc tối đa
bằng `CONTROL_TIMEOUT`. Nhánh đang thức vẫn chuyển tiếp body trực tiếp.

Các lần đọc `/is_sleeping` dùng kết nối mới để tránh keep-alive hết hạn trong
thời gian chờ idle. Benchmark vẫn tái sử dụng kết nối; không tự retry request
suy luận hoặc che lỗi HTTP khi đọc trạng thái.
