"""Kiểm tra vLLM thật qua gateway. Không gọi POST sleep/wake thủ công."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import threading
import time
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
import traceback

from common import Client, summarize

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default="http://gateway:8000")
    parser.add_argument("--direct", default="http://vllm:8000")
    parser.add_argument("--idle-seconds", type=float, default=10)
    parser.add_argument("--requests", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--model")
    parser.add_argument("--suite", choices=("benchmark", "lifecycle"), default="lifecycle")
    parser.add_argument("--report-dir", default=os.environ.get("REPORT_DIR", "results"))
    args = parser.parse_args()
    if min(args.requests, args.concurrency, args.cycles) < 1 or args.idle_seconds < 3:
        parser.error("requests/concurrency/cycles >= 1, idle-seconds >= 3")
    if args.timeout <= args.idle_seconds:
        parser.error("timeout phải lớn hơn idle-seconds")
    key = os.environ.get("OPENAI_API_KEY", "dummy")
    gateway = Client(args.gateway, key, args.timeout)
    direct = Client(args.direct, key, args.timeout)
    report = {"mode": "live", "started": time.time(), "config": vars(args), "checks": [],
              "benchmarks": {}, "state_samples": [],
              "notes": [
                  "Không gọi sleep/wake thủ công: mọi chuyển trạng thái do gateway.",
                  "TTFT gồm sinh token, hàng đợi và mạng; delta percentile không phải overhead riêng.",
                  "Số lần wake chính xác và fault injection được kiểm tra trong mock suite.",
                  "Benchmark yêu cầu IDLE_TIMEOUT=0 để direct request không bị auto-sleep.",
                  "Đọc trạng thái dùng kết nối mới; benchmark vẫn dùng keep-alive và không retry.",
              ]}
    output = Path(args.report_dir)
    output.mkdir(parents=True, exist_ok=True)

    def check(name, function):
        print(f"\n[RUN] {name}", flush=True)
        start = time.monotonic()
        try:
            detail = function()
            status = "SKIP" if isinstance(detail, dict) and detail.get("skip") else "PASS"
            report["checks"].append({"name": name, "status": status, "detail": detail,
                                     "duration_s": time.monotonic() - start})
            print(f"[{status}] {name}: {json.dumps(detail, ensure_ascii=False)}", flush=True)
            return detail
        except Exception:
            report["checks"].append({"name": name, "status": "FAIL",
                                     "trace": traceback.format_exc(),
                                     "duration_s": time.monotonic() - start})
            print(f"[FAIL] {name}\n{traceback.format_exc()}", flush=True)
            raise

    def sleeping():
        value = direct.json("/is_sleeping", fresh=True).get("is_sleeping")
        if not isinstance(value, bool):
            raise AssertionError("/is_sleeping thiếu boolean")
        report["state_samples"].append({"time": time.time(), "asleep": value})
        return value

    def wait_sleep(discovery=False):
        start = time.monotonic()
        while time.monotonic() - start < args.timeout:
            if discovery:
                assert gateway.request("GET", "/healthz")[0] == 200
                assert gateway.request("GET", "/v1/models")[0] == 200
            if sleeping():
                return time.monotonic() - start
            time.sleep(.2)
        raise AssertionError("Không tự sleep; kiểm tra idle, client khác và log auto-sleep disabled")

    def generate(**kwargs):
        return gateway.generate(model, **kwargs)

    def batch(count):
        barrier = threading.Barrier(count)
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            def call():
                barrier.wait(timeout=10)
                return generate()
            futures = [pool.submit(call) for _ in range(count)]
            return [f.result(timeout=args.timeout + 10) for f in futures]

    model = None
    try:
        def readiness():
            nonlocal model
            model = args.model or gateway.json("/v1/models")["data"][0]["id"]
            # Retry chỉ trong bước chờ startup. Không retry các phép đo hoặc test.
            deadline = time.monotonic() + args.timeout
            while True:
                try:
                    result = generate()
                    break
                except RuntimeError as exc:
                    if "HTTP 503" not in str(exc) or time.monotonic() >= deadline:
                        raise
                    time.sleep(2)
            assert not sleeping()
            return {"model": model, "initial_request": result}
        check("readiness", readiness)

        def routes():
            for method, path, expected in [
                ("POST", "/sleep?level=1", 404), ("POST", "/wake_up", 404),
                ("GET", "/is_sleeping", 404), ("GET", "/metrics", 404),
                ("POST", "/v1/responses", 501), ("POST", "/v1/batches", 501)]:
                status, _, _ = gateway.request(method, path)
                assert status == expected, f"{path}: {status}, cần {expected}"
            status, body, _ = gateway.request("POST", "/v1/chat/completions", {
                "model": model, "messages": [{"role": "user", "content": "Say OK."}],
                "max_tokens": 8, "stream": False})
            assert status == 200, body[:300]
            assert json.loads(body).get("choices")
            return {"control_routes": "blocked", "non_streaming": "OK"}
        check("routes_and_non_streaming", routes)

        def awake_benchmark():
            results = {}
            # Cả serial và tải đồng thời; cùng pool giữ connection qua các batch.
            for concurrency in sorted({1, args.concurrency}):
                samples = {"direct": [], "gateway": []}
                light = {"direct": [], "gateway": []}
                generate()  # đảm bảo awake trước khi gọi thẳng backend
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                    for client in (gateway, direct):
                        list(pool.map(lambda _: client.generate(model), range(concurrency)))
                    remaining = args.requests
                    round_index = 0
                    while remaining:
                        # Không direct inference nếu trạng thái vừa đổi thành sleep.
                        generate()
                        assert not sleeping()
                        count = min(remaining, concurrency)
                        order = ("direct", "gateway") if round_index % 2 == 0 else ("gateway", "direct")
                        for name in order:
                            client = direct if name == "direct" else gateway
                            futures = [pool.submit(client.generate, model) for _ in range(count)]
                            samples[name].extend(f.result() for f in futures)
                        remaining -= count
                        round_index += 1
                # Nhẹ: /v1/models không GPU, cùng máy client/đường mạng, keep-alive.
                for i in range(40):
                    for name in (("direct", "gateway") if i % 2 == 0 else ("gateway", "direct")):
                        client = direct if name == "direct" else gateway
                        status, _, elapsed = client.request("GET", "/v1/models")
                        assert status == 200
                        light[name].append(elapsed)
                summary = {name: summarize(rows) for name, rows in samples.items()}
                from common import percentile
                light_summary = {name: {"p50": percentile(rows, .5), "p95": percentile(rows, .95)}
                                 for name, rows in light.items()}
                delta = {field: {p: summary["gateway"][field][p] - summary["direct"][field][p]
                                 for p in ("p50", "p95")} for field in ("ttft_ms", "total_ms")}
                results[str(concurrency)] = {"samples": samples, "summary": summary,
                    "delta_ms": delta, "models_latency_ms": light_summary,
                    "models_samples_ms": light}
                print(f"concurrency={concurrency}: delta(ms)={delta}", flush=True)
            report["benchmarks"] = results
            return {c: r["summary"] for c, r in results.items()}
        if args.suite == "benchmark":
            check("awake_latency_serial_and_concurrent", awake_benchmark)

        def reset_idle():
            generate()
            time.sleep(args.idle_seconds * .55)
            assert not sleeping(), "Sleep trước idle"
            last = generate()
            time.sleep(args.idle_seconds * .55)
            assert not sleeping(), "Request mới không reset idle"
            return {"last_request": last}
        if args.suite == "lifecycle":
            check("new_request_resets_idle", reset_idle)

        def lifecycle():
            cycles = []
            for _ in range(args.cycles):
                generate()
                idle_start = time.monotonic()
                elapsed = wait_sleep(discovery=True)
                # Cho phép sai số thời điểm EOF và polling nhưng không sleep quá sớm.
                assert elapsed >= args.idle_seconds - 1, f"Sleep quá sớm: {elapsed:.3f}s"
                assert sleeping()
                # GET models/healthz trong lúc asleep không được đánh thức.
                for _ in range(3):
                    assert gateway.request("GET", "/v1/models")[0] == 200
                    assert gateway.request("GET", "/healthz")[0] == 200
                assert sleeping(), "Discovery đánh thức backend"
                results = batch(args.concurrency)
                assert not sleeping()
                warm = generate()
                cycles.append({"idle_to_sleep_s": elapsed,
                    "sleep_observed_at_s": time.monotonic() - idle_start,
                    "wake_burst": results, "wake_burst_summary": summarize(results),
                    "awake_after_wake": warm})
            return cycles
        if args.suite == "lifecycle":
            check("automatic_sleep_discovery_and_concurrent_wake", lifecycle)

        def active_stream():
            generate()  # wake trước để thời gian đo chỉ bao gồm stream
            first = threading.Event()
            done = threading.Event()
            begin = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                def call():
                    try:
                        return generate(max_tokens=1024,
                            prompt="Write a long detailed numbered tutorial on Python programming. "
                                   "Continue with at least 100 examples and explanations.",
                            first_token=first.set, extra={"ignore_eos": True})
                    finally:
                        done.set()
                future = pool.submit(call)
                observed = []
                while not done.wait(.2):
                    value = sleeping()
                    if not done.is_set():
                        observed.append(value)
                        assert not value, "Backend sleep khi stream còn đang chạy"
                result = future.result()
            duration = time.monotonic() - begin
            assert first.is_set()
            assert result["total_ms"] > result["ttft_ms"]
            assert result["chunks"] > 1
            if duration < args.idle_seconds * 1.2:
                return {"skip": True, "reason": "Stream ngắn hơn idle; chưa chứng minh active guard",
                        "duration_s": duration, "stream": result}
            elapsed = wait_sleep()
            assert elapsed >= args.idle_seconds - 1, "Idle không tính từ EOF"
            return {"duration_s": duration, "polls": len(observed),
                    "idle_after_eof_s": elapsed, "stream": result}
        if args.suite == "lifecycle":
            check("no_sleep_during_long_stream", active_stream)
    except Exception:
        report["run_error"] = traceback.format_exc()
    finally:
        # Best effort đưa backend về awake sau test; không dùng control thủ công.
        if model:
            try:
                report["cleanup_request"] = generate()
            except Exception as exc:
                report["cleanup_error"] = str(exc)
        gateway.close()
        direct.close()
        report["success"] = "run_error" not in report and "cleanup_error" not in report and bool(report["checks"]) and all(
            c["status"] != "FAIL" for c in report["checks"])
        report["finished"] = time.time()
        path = output / ("live-" + args.suite + "-report.json")
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nBáo cáo: {path}", flush=True)
    return 0 if report["success"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
