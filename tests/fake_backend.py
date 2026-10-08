"""Backend giả có lịch sử event để kiểm tra chính xác thứ tự sleep/wake."""
import json
import threading
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Backend:
    def __init__(self):
        self.lock = threading.Lock()
        self.asleep = False
        self.events = []
        self.active = 0
        self.max_active = 0
        self.wake_delay = .12
        self.sleep_delay = .02
        self.token_delay = .02
        self.token_count = 4
        self.wake_status = 200
        self.sleep_status = 200
        self.state_status = 200
        self.early_wake = False
        self.close_after_reply = False
        self.generation_started = threading.Event()

    def record(self, kind, **fields):
        with self.lock:
            self.events.append({"kind": kind, "time": time.monotonic(), **fields})

    def history(self, kind):
        with self.lock:
            return [dict(e) for e in self.events if e["kind"] == kind]

    def sleeping(self):
        with self.lock:
            return self.asleep

    def set_sleeping(self, value):
        with self.lock:
            self.asleep = value

    def server(self):
        backend = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            def log_message(self, *args):
                pass

            def reply(self, status, body):
                data = json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    if backend.close_after_reply:
                        # Simulate a peer silently expiring its keep-alive.
                        self.close_connection = True
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    # Control timeout tests intentionally close the caller socket.
                    backend.record("control_disconnect", path=self.path)
                    self.close_connection = True

            def do_GET(self):
                if self.path == "/is_sleeping":
                    backend.record("state")
                    self.reply(backend.state_status, {"is_sleeping": backend.sleeping()})
                elif self.path == "/health":
                    self.reply(200, {})
                elif self.path.startswith("/v1/models"):
                    self.reply(200, {"data": [{"id": "fake-model"}]})
                else:
                    self.reply(404, {})

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if self.path.startswith("/sleep"):
                    backend.record("sleep_start", path=self.path, active=backend.active)
                    time.sleep(backend.sleep_delay)
                    if backend.sleep_status == 200:
                        backend.set_sleeping(True)
                    backend.record("sleep_end")
                    self.reply(backend.sleep_status, {})
                elif self.path == "/wake_up":
                    backend.record("wake_start")
                    def finish():
                        time.sleep(backend.wake_delay)
                        if backend.wake_status == 200:
                            backend.set_sleeping(False)
                        backend.record("wake_end")
                    if backend.early_wake:
                        threading.Thread(target=finish, daemon=True).start()
                    else:
                        finish()
                    self.reply(backend.wake_status, {})
                elif self.path == "/v1/chat/completions":
                    if backend.sleeping():
                        backend.record("bad_inference")
                        self.reply(503, {"error": "asleep"})
                        return
                    payload = json.loads(body)
                    backend.record("inference", authorization=self.headers.get("Authorization"),
                                   payload=payload)
                    with backend.lock:
                        backend.active += 1
                        backend.max_active = max(backend.max_active, backend.active)
                    backend.generation_started.set()
                    try:
                        if not payload.get("stream"):
                            self.reply(200, {"choices": [{"message": {"content": "OK"}}]})
                            return
                        self.send_response(200)
                        self.send_header("Content-Type", "text/event-stream")
                        self.send_header("Transfer-Encoding", "chunked")
                        self.end_headers()
                        def send(data):
                            self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
                            self.wfile.flush()
                        for _ in range(backend.token_count):
                            time.sleep(backend.token_delay)
                            event = {"choices": [{"delta": {"content": "OK "}}]}
                            send(b"data: " + json.dumps(event).encode() + b"\n\n")
                        send(b"data: [DONE]\n\n")
                        self.wfile.write(b"0\r\n\r\n")
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                        backend.record("disconnect")
                    finally:
                        with backend.lock:
                            backend.active -= 1
                        backend.record("inference_end")
                else:
                    self.reply(404, {})
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        return server
