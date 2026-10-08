"""Compare direct vLLM and an awake gateway. Use only with IDLE_TIMEOUT=0."""
import argparse
import concurrent.futures
import http.client
import json
import math
import os
import threading
import time
from urllib.parse import urlsplit

_local = threading.local()
_connections = []
_connection_lock = threading.Lock()


def connection(base):
    parsed = urlsplit(base.rstrip("/"))
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Endpoints must be HTTP(S) URLs")
    connections = getattr(_local, "connections", None)
    if connections is None:
        connections = _local.connections = {}
    if base not in connections:
        cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = cls(parsed.hostname, parsed.port, timeout=120)
        connections[base] = conn
        with _connection_lock:
            _connections.append(conn)
    return connections[base], parsed.path.rstrip("/")


def models(base, key):
    conn, prefix = connection(base)
    conn.request("GET", prefix + "/v1/models", headers={"Authorization": "Bearer " + key})
    response = conn.getresponse()
    data = response.read()
    if response.status != 200:
        raise RuntimeError(f"Model discovery failed: HTTP {response.status}: {data[:300]!r}")
    return json.loads(data)["data"][0]["id"]


def generate(base, key, model):
    conn, prefix = connection(base)
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": "Reply with exactly: Hello world."}],
        "max_tokens": 16,
        "temperature": 0,
        "stream": True,
    })
    started = time.perf_counter()
    conn.request("POST", prefix + "/v1/chat/completions", body=payload, headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + key,
    })
    response = conn.getresponse()
    if response.status != 200:
        data = response.read()
        raise RuntimeError(f"{base}: HTTP {response.status}: {data[:300]!r}")
    first_token = None
    saw_done = False
    # Read to EOF, not just [DONE], so the gateway sees a completed response.
    for raw in response:
        if not raw.startswith(b"data:"):
            continue
        value = raw[5:].strip()
        if value == b"[DONE]":
            saw_done = True
            continue
        if not value:
            continue
        event = json.loads(value)
        if "error" in event:
            raise RuntimeError(str(event["error"]))
        for choice in event.get("choices", []):
            delta = choice.get("delta", {})
            if first_token is None and (delta.get("content") or choice.get("text")):
                first_token = time.perf_counter()
    finished = time.perf_counter()
    if first_token is None or not saw_done:
        raise RuntimeError(f"{base}: incomplete stream or no text token")
    return (first_token - started) * 1000, (finished - started) * 1000


def percentile(values, fraction):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--direct", required=True)
    parser.add_argument("--gateway", required=True)
    parser.add_argument("--model")
    parser.add_argument("--requests", type=int, default=20, help="Measured requests per endpoint")
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("--requests and --concurrency must be positive")
    key = os.environ.get("OPENAI_API_KEY", "dummy")
    model = args.model or models(args.gateway, key)
    endpoints = {"direct": args.direct, "gateway": args.gateway}
    results = {name: [] for name in endpoints}

    # Gateway first: establish readiness/wake even if the backend was asleep
    # before IDLE_TIMEOUT was disabled.
    generate(args.gateway, key, model)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for base in (args.gateway, args.direct):
            list(pool.map(lambda _: generate(base, key, model), range(args.concurrency)))
        remaining = args.requests
        round_index = 0
        while remaining:
            count = min(remaining, args.concurrency)
            order = ("direct", "gateway") if round_index % 2 == 0 else ("gateway", "direct")
            for name in order:
                futures = [pool.submit(generate, endpoints[name], key, model) for _ in range(count)]
                results[name].extend(future.result() for future in futures)
            remaining -= count
            round_index += 1

    print(f"model={model}, requests/path={args.requests}, concurrency={args.concurrency}")
    print("path        TTFT p50(ms)  TTFT p95(ms)  total p50(ms) total p95(ms)")
    for name, samples in results.items():
        ttft, total = zip(*samples)
        print(f"{name:<11} {percentile(ttft, .50):>12.3f} {percentile(ttft, .95):>13.3f}"
              f" {percentile(total, .50):>14.3f} {percentile(total, .95):>13.3f}")
    print("Includes model execution, scheduling and client/network overhead; not an isolated proxy measurement.")


if __name__ == "__main__":
    try:
        main()
    finally:
        for conn in _connections:
            conn.close()
