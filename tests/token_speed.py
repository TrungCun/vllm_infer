"""Đo tốc độ output của một request streaming qua gateway."""
import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from common import Client


def measure(index, client, payload, barrier):
    try:
        barrier.wait(timeout=15)
        conn = client.connection()
        start = time.perf_counter()
        conn.request("POST", urlsplit(client.base).path.rstrip("/") + "/v1/chat/completions",
                     json.dumps(payload).encode(),
                     {"Content-Type": "application/json", "Authorization": "Bearer " + client.key})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {response.read()[:500]!r}")
        first = last = None
        usage = None
        done = False
        parts = []
        finish_reason = None
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
            if event.get("usage"):
                usage = event["usage"]
            for choice in event.get("choices", []):
                if choice.get("finish_reason"):
                    finish_reason = choice["finish_reason"]
                content = choice.get("delta", {}).get("content")
                if content:
                    last = time.perf_counter()
                    if first is None:
                        first = last
                        print(f"Token đầu tiên sau {(first-start):.3f}s", flush=True)
                    parts.append(content)
        end = time.perf_counter()
        if not done or first is None or not usage or not usage.get("completion_tokens"):
            raise RuntimeError("Stream thiếu nội dung, [DONE] hoặc usage.completion_tokens; không thể đo token/s chính xác.")
        tokens = usage["completion_tokens"]
        decode_seconds = last - first
        report = {"timestamp": datetime.now().astimezone().isoformat(),
                  "base_url": client.base, "request": payload,
                  "ttft_ms": (first-start)*1000, "total_ms": (end-start)*1000,
                  "output_tokens": tokens, "prompt_tokens": usage.get("prompt_tokens"),
                  "decode_seconds": decode_seconds,
                  "decode_tokens_per_second": (tokens-1)/decode_seconds if decode_seconds > 0 else None,
                  "end_to_end_tokens_per_second": tokens/(end-start),
                  "finish_reason": finish_reason, "output": "".join(parts),
                  "note": "Decode token/s là ước lượng phía client: (output_tokens-1)/(thời điểm chunk nội dung cuối - đầu). TTFT có thể gồm wake-up nếu backend đang sleep."}
        report["request_index"] = index
        report["started_at_monotonic"] = start
        report["first_token_at_monotonic"] = first
        report["last_token_at_monotonic"] = last
        report["ended_at_monotonic"] = end
        return report
    finally:
        client.close()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Đo token output qua gateway với nhiều request đồng thời.")
    parser.add_argument("--base-url", default="http://127.0.0.1:9094")
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--prompt", default="Viết một bài giải thích chi tiết bằng tiếng Việt về cách continuous batching giúp phục vụ nhiều request LLM đồng thời. Trình bày prefill, decode, KV cache và ví dụ thực tế. Viết khoảng 800 từ.")
    args = parser.parse_args()
    if args.max_tokens < 2 or args.concurrency < 1:
        parser.error("--max-tokens phải >= 2 và --concurrency phải >= 1")
    key = os.environ.get("OPENAI_API_KEY", "dummy")
    discovery = Client(args.base_url, key=key)
    try:
        model = discovery.json("/v1/models")["data"][0]["id"]
    finally:
        discovery.close()
    payload = {"model": model, "messages": [{"role": "user", "content": args.prompt}],
               "max_tokens": args.max_tokens, "temperature": 0, "stream": True,
               "stream_options": {"include_usage": True}}
    barrier = threading.Barrier(args.concurrency)
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(measure, i+1, Client(args.base_url, key=key, timeout=180), payload, barrier)
                   for i in range(args.concurrency)]
        results = [f.result() for f in futures]
    total_tokens = sum(r["output_tokens"] for r in results)
    elapsed = max(r["ended_at_monotonic"] for r in results)-min(r["started_at_monotonic"] for r in results)
    decode_elapsed = max(r["last_token_at_monotonic"] for r in results)-min(r["first_token_at_monotonic"] for r in results)
    summary = {"concurrency": args.concurrency, "total_output_tokens": total_tokens,
               "wall_seconds": elapsed, "aggregate_end_to_end_tokens_per_second": total_tokens/elapsed,
               "aggregate_decode_tokens_per_second": (total_tokens-args.concurrency)/decode_elapsed,
               "request_start_spread_ms": (max(r["started_at_monotonic"] for r in results)-min(r["started_at_monotonic"] for r in results))*1000}
    report = {"summary": summary, "requests": results}
    folder = Path(__file__).resolve().parent / "results" / datetime.now().strftime("%Y%m%d-%H%M%S-token-speed")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "token-speed-report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for r in results:
        print(json.dumps({k:r[k] for k in ("request_index", "output_tokens", "ttft_ms", "total_ms", "decode_tokens_per_second", "end_to_end_tokens_per_second", "finish_reason")}, ensure_ascii=False))
    print(f"Báo cáo: {path}")


if __name__ == "__main__":
    main()
