"""Interop probe (Python) for the R1 Redis scenarios.

Environment contract and output lines: see ../go/main.go.
"""
import json
import logging
import os
import sys
import time
from urllib.parse import urlencode

from babelqueue.redis_transport import RedisTransport

logging.basicConfig(stream=sys.stderr, level=logging.WARNING, format="PROBE python WARN %(name)s: %(message)s")


def build_url() -> str:
    base = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    params = {}
    if os.environ.get("LAYOUT", "zset").lower() == "zset":
        params["laravel"] = "1"
    retry_after = os.environ.get("RETRY_AFTER", "")
    if retry_after == "none":
        params["retry_after"] = "null"
    elif retry_after:
        params["retry_after"] = str(int(float(retry_after)))
    if not params:
        return base
    return base + ("&" if "?" in base else "?") + urlencode(params)


def main() -> int:
    queue = os.environ.get("QUEUE", "orders")
    mode = os.environ.get("MODE", "consume")
    hold = float(os.environ.get("HOLD", "0"))
    timeout = float(os.environ.get("TIMEOUT", "30"))

    transport = RedisTransport(build_url())
    print("PROBE python READY", flush=True)

    deadline = time.time() + timeout
    while time.time() < deadline:
        message = transport.pop(queue, 1.0)
        if message is None:
            continue
        received_ms = int(time.time() * 1000)
        envelope = json.loads(message.body)
        message_id = (envelope.get("meta") or {}).get("id", "")
        print(
            f"PROBE python RECEIVED id={message_id} attempts={envelope.get('attempts')} at_ms={received_ms}",
            flush=True,
        )
        if mode == "hang":
            while True:  # the runner kill -9's us while the reservation is held
                time.sleep(3600)
        if mode == "slow":
            time.sleep(hold)
        transport.ack(message)
        print(f"PROBE python ACKED id={message_id}", flush=True)
        return 0

    print("PROBE python TIMEOUT", flush=True)
    return 3


if __name__ == "__main__":
    sys.exit(main())
