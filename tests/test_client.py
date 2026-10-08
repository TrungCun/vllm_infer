"""Kiểm tra công cụ đo: TTFT, EOF, lỗi HTTP và connection từng thread."""
import concurrent.futures
import threading
import unittest

from common import Client, percentile, wait_until
from fake_backend import Backend

class ClientTests(unittest.TestCase):
    def setUp(self):
        self.backend = Backend()
        self.server = self.backend.server()
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.client = Client(f"http://127.0.0.1:{self.server.server_port}")

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()

    def test_ttft_before_eof_and_full_response(self):
        result = self.client.generate("fake-model")
        self.assertEqual(result["text"], "OK " * 4)
        self.assertEqual(result["chunks"], 4)
        self.assertGreater(result["total_ms"] - result["ttft_ms"], 30)
        self.client.generate("fake-model")  # reuse connection sau EOF

    def test_http_error_is_not_latency_sample(self):
        self.backend.set_sleeping(True)
        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            self.client.generate("fake-model")

    def test_no_content_is_not_success(self):
        self.backend.token_count = 0
        with self.assertRaisesRegex(RuntimeError, "thiếu token"):
            self.client.generate("fake-model")

    def test_expected_control_timeout_is_recorded_without_traceback(self):
        self.backend.sleep_delay = .15
        caller = Client(self.client.base, timeout=.02)
        try:
            with self.assertRaises(TimeoutError):
                caller.request("POST", "/sleep?level=1")
        finally:
            caller.close()
        wait_until(lambda: self.backend.history("control_disconnect"), timeout=2)
        self.assertTrue(self.backend.sleeping())

    def test_fresh_state_read_after_peer_closes_keepalive(self):
        import select
        import http.client
        self.backend.close_after_reply = True
        self.assertFalse(self.client.json("/is_sleeping")["is_sleeping"])
        sock = self.client.connection().sock
        # Wait until FIN arrives so this regression does not depend on sleep.
        readable, _, _ = select.select([sock], [], [], 2)
        self.assertTrue(readable, "peer did not close the connection")
        with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
            self.client.json("/is_sleeping")  # old implementation reproduces failure
        for _ in range(3):
            self.assertFalse(self.client.json("/is_sleeping", fresh=True)["is_sleeping"])
            self.assertIsNone(self.client.connection().sock)

    def test_fresh_request_does_not_retry_http_error(self):
        self.backend.state_status = 503
        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            self.client.json("/is_sleeping", fresh=True)
        self.assertEqual(len(self.backend.history("state")), 1)

    def test_concurrent_requests_and_percentiles(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.client.generate("fake-model"), range(8)))
        self.assertEqual(len(results), 8)
        self.assertGreater(self.backend.max_active, 1)
        self.assertEqual(percentile([4, 1, 3, 2], .5), 2)
        self.assertEqual(percentile([4, 1, 3, 2], .95), 4)

if __name__ == "__main__":
    unittest.main(verbosity=2)
