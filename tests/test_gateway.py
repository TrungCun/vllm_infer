"""Integration test: chạy binary gateway thật với backend HTTP giả."""
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
import unittest

from common import Client, summarize, wait_until
from fake_backend import Backend

REPORT = Path(os.environ.get("REPORT_DIR", "results"))
BENCHMARK = {}

class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.backend = Backend()
        self.server = self.backend.server()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.direct = Client(f"http://127.0.0.1:{self.server.server_port}", timeout=5)
        self.process = None
        self.gateway = None
        self.log_path = None
        self.log = None
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=24)

    def start(self, idle=".3s", control="2s"):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        REPORT.mkdir(parents=True, exist_ok=True)
        self.log_path = REPORT / (self.id().split(".")[-1] + ".log")
        self.log = self.log_path.open("w", encoding="utf-8")
        env = dict(os.environ, UPSTREAM_URL=self.direct.base, IDLE_TIMEOUT=idle,
                   CONTROL_TIMEOUT=control, LISTEN_ADDR=f"127.0.0.1:{port}")
        self.process = subprocess.Popen([os.environ.get("GATEWAY_BIN", "/usr/local/bin/gateway")],
                                        env=env, stdout=self.log, stderr=self.log)
        self.gateway = Client(f"http://127.0.0.1:{port}", timeout=5)
        def ready():
            if self.process.poll() is not None:
                raise AssertionError(self.logs())
            try:
                return self.gateway.request("GET", "/healthz")[0] == 200
            except OSError:
                # HTTPConnection có thể dùng lại sau connect thất bại.
                return False
        wait_until(ready)
        return self.gateway

    def logs(self):
        return self.log_path.read_text(encoding="utf-8") if self.log_path else ""

    def generate(self):
        return self.gateway.generate("fake-model")

    def burst(self, count=16):
        barrier = threading.Barrier(count)
        def call():
            barrier.wait(timeout=5)
            return self.generate()
        return [self.pool.submit(call) for _ in range(count)]

    def tearDown(self):
        self.pool.shutdown(wait=True)
        if self.gateway:
            self.gateway.close()
        self.direct.close()
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
        self.server.shutdown()
        self.server.server_close()

    def test_awake_fast_path_and_forwarding(self):
        self.start(idle="0")
        self.generate()  # readiness initialization
        before = len(self.backend.history("state"))
        result = self.generate()
        self.assertEqual(result["text"], "OK " * 4)
        self.assertLess(result["ttft_ms"], result["total_ms"] - 20)
        self.assertEqual(len(self.backend.history("state")), before)
        self.assertFalse(self.backend.history("wake_start"))
        event = self.backend.history("inference")[-1]
        self.assertEqual(event["authorization"], "Bearer dummy")
        self.assertEqual(event["payload"]["model"], "fake-model")
        status, body, _ = self.gateway.request("POST", "/v1/chat/completions",
            {"model": "fake-model", "messages": [], "stream": False})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["choices"][0]["message"]["content"], "OK")

    def test_concurrent_requests_share_one_wake(self):
        self.backend.set_sleeping(True)
        self.backend.wake_delay = .3
        self.start()
        results = [f.result(timeout=5) for f in self.burst()]
        self.assertEqual(len(results), 16)
        self.assertEqual(len(self.backend.history("wake_start")), 1)
        self.assertFalse(self.backend.history("bad_inference"))
        self.assertGreater(self.backend.max_active, 1)
        awake = self.backend.history("wake_end")[0]["time"]
        self.assertTrue(all(e["time"] >= awake for e in self.backend.history("inference")))

    def test_idle_deadline_and_repeated_cycles(self):
        self.start()
        for cycle in range(3):
            self.generate()
            end = self.backend.history("inference_end")[-1]["time"]
            time.sleep(.12)
            self.assertFalse(self.backend.sleeping())
            wait_until(self.backend.sleeping)
            sleeps = self.backend.history("sleep_start")
            self.assertEqual(len(sleeps), cycle + 1)
            self.assertGreaterEqual(sleeps[-1]["time"] - end, .27)
            self.assertEqual(sleeps[-1]["path"], "/sleep?level=1")
            self.assertEqual(sleeps[-1]["active"], 0)
        self.assertEqual(len(self.backend.history("wake_start")), 2)

    def test_no_sleep_during_stream_then_idle_from_eof(self):
        self.backend.token_delay = .1
        self.backend.token_count = 8
        self.start(idle=".2s")
        first = threading.Event()
        future = self.pool.submit(self.gateway.generate, "fake-model", first_token=first.set)
        self.assertTrue(first.wait(3))
        time.sleep(.35)  # stream kéo dài hơn idle
        self.assertFalse(self.backend.history("sleep_start"))
        self.assertFalse(future.done())
        future.result(timeout=3)
        end = self.backend.history("inference_end")[-1]["time"]
        wait_until(self.backend.sleeping)
        self.assertGreaterEqual(self.backend.history("sleep_start")[0]["time"] - end, .18)

    def test_request_arriving_during_sleep_waits_then_wakes(self):
        self.backend.sleep_delay = .4
        self.start(idle=".15s")
        self.generate()
        wait_until(lambda: self.backend.history("sleep_start"))
        self.generate()
        self.assertFalse(self.backend.history("bad_inference"))
        self.assertEqual(len(self.backend.history("wake_start")), 1)
        self.assertGreaterEqual(self.backend.history("wake_start")[0]["time"],
                                self.backend.history("sleep_end")[0]["time"])

    def test_discovery_does_not_reset_idle_or_wake(self):
        self.start(idle=".2s")
        self.generate()
        for _ in range(8):
            self.assertEqual(self.gateway.request("GET", "/v1/models")[0], 200)
            self.assertEqual(self.gateway.request("GET", "/healthz")[0], 200)
            time.sleep(.05)
        self.assertTrue(self.backend.sleeping())
        self.assertFalse(self.backend.history("wake_start"))

    def test_request_resets_idle_clock(self):
        self.start(idle=".3s")
        self.generate()
        time.sleep(.18)
        self.generate()
        time.sleep(.18)
        self.assertFalse(self.backend.sleeping())
        wait_until(self.backend.sleeping)

    def test_disabled_idle_and_parallel_awake_requests(self):
        self.start(idle="0")
        self.generate()
        [f.result(timeout=5) for f in self.burst()]
        time.sleep(.4)
        self.assertFalse(self.backend.history("sleep_start"))
        self.assertGreater(self.backend.max_active, 1)

    def test_wake_acknowledgement_is_not_readiness(self):
        self.backend.set_sleeping(True)
        self.backend.early_wake = True
        self.backend.wake_delay = .4
        self.start()
        self.generate()
        self.assertFalse(self.backend.history("bad_inference"))
        self.assertGreaterEqual(self.backend.history("inference")[0]["time"],
                                self.backend.history("wake_end")[0]["time"])

    def assert_faulted(self, message):
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
                self.generate()
        self.assertFalse(self.backend.history("inference"), message)

    def test_wake_failure_faults_without_retry(self):
        self.backend.set_sleeping(True)
        self.backend.wake_status = 500
        self.start()
        self.assert_faulted("inference forwarded on failed wake")
        self.assertEqual(len(self.backend.history("wake_start")), 1)

    def test_wake_timeout_faults_without_retry(self):
        self.backend.set_sleeping(True)
        self.backend.wake_delay = .4
        self.start(control=".1s")
        self.assert_faulted("inference forwarded on timed out wake")
        self.assertEqual(len(self.backend.history("wake_start")), 1)
        time.sleep(.45)  # backend kết thúc operation cũ trước teardown

    def test_sleep_failure_faults_without_wake(self):
        self.backend.sleep_status = 500
        self.start(idle=".15s")
        self.generate()
        wait_until(lambda: "sleep failed" in self.logs())
        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            self.generate()
        self.assertFalse(self.backend.history("wake_start"))
        self.assertEqual(len(self.backend.history("sleep_start")), 1)

    def test_initial_readiness_failure_can_retry(self):
        self.backend.state_status = 503
        self.start()
        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            self.generate()
        self.backend.state_status = 200
        self.generate()
        self.assertEqual(len(self.backend.history("inference")), 1)

    def test_internal_and_background_routes_blocked(self):
        self.start()
        for method, path, expected in [
            ("POST", "/sleep?level=1", 404), ("POST", "/wake_up", 404),
            ("GET", "/is_sleeping", 404), ("GET", "/metrics", 404),
            ("POST", "/v1/responses", 501), ("POST", "/v1/batches", 501)]:
            self.assertEqual(self.gateway.request(method, path)[0], expected)
        self.assertFalse(self.backend.history("inference"))
        self.assertFalse(self.backend.history("state"))

    def test_cancelled_stream_disables_sleep_but_keeps_serving(self):
        self.backend.token_delay = .1
        self.backend.token_count = 10
        self.start(idle=".15s")
        # Cố ý ngắt TCP sau token đầu.
        import http.client
        from urllib.parse import urlsplit
        url = urlsplit(self.gateway.base)
        conn = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
        conn.request("POST", "/v1/chat/completions", json.dumps({
            "model": "fake-model", "messages": [], "stream": True}),
            {"Content-Type": "application/json"})
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        self.assertTrue(response.readline().startswith(b"data:"))
        response.close()
        conn.close()
        wait_until(lambda: "auto-sleep disabled" in self.logs())
        time.sleep(.4)
        self.assertFalse(self.backend.history("sleep_start"))
        self.generate()
        time.sleep(.25)
        self.assertFalse(self.backend.history("sleep_start"))

    def test_sleep_timeout_faults_without_retry(self):
        self.backend.sleep_delay = .4
        self.start(idle=".15s", control=".1s")
        self.generate()
        wait_until(lambda: "sleep failed" in self.logs())
        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            self.generate()
        time.sleep(.45)
        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            self.generate()
        self.assertEqual(len(self.backend.history("sleep_start")), 1)
        self.assertFalse(self.backend.history("wake_start"))

    def test_cancelled_waiter_does_not_cancel_shared_wake(self):
        self.backend.set_sleeping(True)
        self.backend.wake_delay = .4
        self.start(idle="0")
        waiter = Client(self.gateway.base, timeout=.05)
        try:
            with self.assertRaises(TimeoutError):
                waiter.generate("fake-model")
        finally:
            waiter.close()
        self.generate()
        self.assertEqual(len(self.backend.history("wake_start")), 1)
        self.assertFalse(self.backend.history("bad_inference"))
        self.assertNotIn("auto-sleep disabled", self.logs())
        self.assertEqual(len(self.backend.history("inference")), 1)

    def test_awake_latency_benchmark(self):
        self.start(idle="0")
        self.backend.token_delay = .001
        self.generate()
        samples = {"direct": [], "gateway": []}
        # Một thread, keep-alive và thứ tự xen kẽ; không coi model giả là GPU.
        for i in range(40):
            order = ("direct", "gateway") if i % 2 == 0 else ("gateway", "direct")
            for name in order:
                client = self.direct if name == "direct" else self.gateway
                samples[name].append(client.generate("fake-model"))
        summary = {name: summarize(rows) for name, rows in samples.items()}
        delta = {metric: {p: summary["gateway"][metric][p] - summary["direct"][metric][p]
                          for p in ("p50", "p95")} for metric in ("ttft_ms", "total_ms")}
        BENCHMARK.update(samples=samples, summary=summary, delta_ms=delta,
                         note="Backend giả; chênh percentile không phải overhead từng request.")
        print("\nĐộ trễ backend giả:", json.dumps(delta, ensure_ascii=False), flush=True)
        # Không assert ngưỡng ms dễ nhiễu bởi Docker/CPU scheduling.

if __name__ == "__main__":
    REPORT.mkdir(parents=True, exist_ok=True)
    started = time.time()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(GatewayTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    report = {"mode": "mock", "timestamp": started, "tests": result.testsRun,
              "success": result.wasSuccessful(),
              "failures": [{"test": str(t), "trace": trace} for t, trace in result.failures],
              "errors": [{"test": str(t), "trace": trace} for t, trace in result.errors],
              "benchmark": BENCHMARK}
    (REPORT / "mock-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2),
                                           encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
