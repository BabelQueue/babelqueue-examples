"""DLQ consumer (Python) — reads the canonical envelopes quarantined on
``orders.dlq`` and triages them. A dead-letter queue is just an ordinary queue
holding canonical envelopes (ADR-0009), so the *same* runtime that consumes
``orders`` consumes ``orders.dlq`` — only the queue name changes. Each envelope
carries the original identity (``trace_id`` / ``meta.id`` / ``data``) verbatim
plus the additive top-level ``dead_letter`` block, which this consumer inspects
to decide what to do.

    pip install "babelqueue[redis]"
    python consume_dlq.py

The handler takes a third argument (``env``) to receive the full envelope — that
is where the ``dead_letter`` block lives. Run ``seed.py`` first to populate the DLQ.
"""

import os

from babelqueue import BabelQueue

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("QUEUE", "orders")
DLQ = f"{QUEUE}.dlq"

app = BabelQueue(BROKER_URL, queue=DLQ)


def triage(urn: str, data: dict, env: dict) -> None:
    """Print one quarantined message, surfacing why it was dead-lettered."""
    dl = env.get("dead_letter") or {}
    print(
        f"[dlq] {urn}\n"
        f"      reason={dl.get('reason')}  attempts={dl.get('attempts')}"
        f"  lang={dl.get('lang')}\n"
        f"      error={dl.get('error')!r}  ({dl.get('exception')})\n"
        f"      original_queue={dl.get('original_queue')}  trace={env.get('trace_id')}\n"
        f"      data={data}"
    )


@app.handler("urn:babel:orders:created")
def on_order_created(data, meta, env):
    triage("urn:babel:orders:created", data, env)


@app.handler("urn:babel:catalog:item.indexed")
def on_item_indexed(data, meta, env):
    triage("urn:babel:catalog:item.indexed", data, env)


if __name__ == "__main__":
    # Drain whatever is currently on the DLQ, then stop (max_messages bounds the
    # loop; it returns once the DLQ drains within one poll timeout). For a
    # long-running triage worker, call app.run() / app.consume() with no bound.
    processed = app.consume(DLQ, max_messages=1000, timeout=1.0)
    print(f"[dlq] triaged {processed} dead-lettered message(s) on '{DLQ}'.")
