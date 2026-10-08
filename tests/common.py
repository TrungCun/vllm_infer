"""HTTP client và báo cáo; chỉ dùng thư viện chuẩn Python."""
import http.client
import json
import math
import threading
import time
from urllib.parse import urlsplit

class Client:
    def __init__(self, base, key="dummy", timeout=120):
        self.base = base.rstrip("/")
        self.key = key
        self.timeout = timeout
        self.local = threading.local()
        self.connections = []
        self.lock = threading.Lock()

    def connection(self):
        if not hasattr(self.local, "conn"):
            url = urlsplit(self.base)
            cls = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
            if url.scheme not in ("http", "https") or not url.hostname:
                raise ValueError("URL phải có http:// hoặc https://")
            self.local.conn = cls(url.hostname, url.port, timeout=self.timeout)
            with self.lock:
                self.connections.append(self.local.conn)
        return self.local.conn

    def request(self, method, path, payload=None, *, fresh=False):
        conn = self.connection()
        if fresh:
            # Control observations can be separated by a long idle wait.
            # A fresh socket avoids reuse of a server-expired keep-alive.
            conn.close()
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"Authorization": "Bearer " + self.key}
        if body is not None:
            headers["Content-Type"] = "application/json"
        start = time.perf_counter()
        try:
            conn.request(method, urlsplit(self.base).path.rstrip("/") + path, body, headers)
            response = conn.getresponse()
            data = response.read()
            return response.status, data, (time.perf_counter() - start) * 1000
        except Exception:
            conn.close()
            raise
        finally:
            if fresh:
                conn.close()

    def json(self, path, *, fresh=False):
        status, body, _ = self.request("GET", path, fresh=fresh)
        if status != 200:
            raise RuntimeError(f"{path}: HTTP {status}: {body[:300]!r}")
        return json.loads(body)

    def generate(self, model, max_tokens=16, prompt="Reply with exactly: Hello world.",
                 first_token=None, extra=None):
        try:
            return self._generate(model, max_tokens, prompt, first_token, extra)
        except Exception:
            self.connection().close()
            raise

    def _generate(self, model, max_tokens, prompt, first_token, extra):
        payload = {"model": model, "messages": [{"role": "user", "content": prompt}],
                   "max_tokens": max_tokens, "temperature": 0, "stream": True}
        if extra:
            payload.update(extra)
        conn = self.connection()
        start = time.perf_counter()
        conn.request("POST", urlsplit(self.base).path.rstrip("/") + "/v1/chat/completions",
                     json.dumps(payload).encode(),
                     {"Authorization": "Bearer " + self.key, "Content-Type": "application/json"})
        response = conn.getresponse()
        if response.status != 200:
            body = response.read()
            raise RuntimeError(f"HTTP {response.status}: {body[:300]!r}")
        if "text/event-stream" not in response.getheader("Content-Type", ""):
            response.read()
            raise RuntimeError("Response không phải SSE")
        ttft = None
        done = False
        text = []
        chunks = 0
        # Đọc đến EOF: đóng sớm ở [DONE] có thể bị coi là client hủy request.
        for raw in response:
            if not raw.startswith(b"data:"):
                continue
            data = raw[5:].strip()
            if data == b"[DONE]":
                done = True
                continue
            if not data:
                continue
            event = json.loads(data)
            if "error" in event:
                raise RuntimeError(str(event["error"]))
            for choice in event.get("choices", []):
                content = choice.get("delta", {}).get("content") or choice.get("text")
                if content:
                    if ttft is None:
                        ttft = (time.perf_counter() - start) * 1000
                        if first_token:
                            first_token()
                    text.append(content)
                    chunks += 1
        if ttft is None or not done:
            raise RuntimeError("Stream thiếu token hoặc [DONE]")
        return {"ttft_ms": ttft, "total_ms": (time.perf_counter() - start) * 1000,
                "chunks": chunks, "text": "".join(text)}

    def close(self):
        for conn in self.connections:
            conn.close()

def percentile(values, fraction):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)]

def summarize(samples):
    return {field: {"p50": percentile([r[field] for r in samples], .5),
                    "p95": percentile([r[field] for r in samples], .95),
                    "min": min(r[field] for r in samples),
                    "max": max(r[field] for r in samples)}
            for field in ("ttft_ms", "total_ms")}

def wait_until(predicate, timeout=10, interval=.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"Điều kiện chưa đạt sau {timeout}s")
