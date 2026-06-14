"""DLQ re-drive (Python) — moves canonical envelopes from ``<queue>.dlq`` back to
the source ``<queue>`` so they get another run, after you have fixed whatever made
them fail. This is the cross-language re-drive case: the DLQ holds plain canonical
envelopes, so the message a *Go* or *PHP* consumer dead-lettered is re-driven here
and can be picked up by a consumer in *any* language.

    pip install "babelqueue[redis]"
    python redrive.py                 # re-drive every message on orders.dlq
    python redrive.py --max 1         # re-drive at most one
    python redrive.py --keep-dead-letter   # leave the dead_letter block in place

Each message is reserved on the DLQ (``pop``), re-published to the source queue,
then acked off the DLQ — so an interrupted run never loses or duplicates messages
beyond the at-least-once guarantee. By default the additive ``dead_letter`` block
is stripped and ``attempts`` reset to 0, giving the message a clean re-run; the
original identity (``trace_id`` / ``meta.id`` / ``data``) is preserved verbatim.
Run only after the underlying fault is fixed, or the message will just fail again.
"""

import argparse
import os

from babelqueue import EnvelopeCodec
from babelqueue.transport import make_transport

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("QUEUE", "orders")


def redrive(broker_url: str, queue: str, *, max_messages=None, keep_dead_letter=False) -> int:
    """Move messages from ``<queue>.dlq`` back to ``<queue>``; return the count."""
    dlq = f"{queue}.dlq"
    transport = make_transport(broker_url)
    moved = 0

    while max_messages is None or moved < max_messages:
        received = transport.pop(dlq, timeout=1.0)
        if received is None:
            break  # DLQ drained within one poll timeout

        envelope = EnvelopeCodec.decode(received.body)
        if not keep_dead_letter:
            envelope.pop("dead_letter", None)
            envelope["attempts"] = 0  # fresh re-run on the source queue
        body = EnvelopeCodec.encode(envelope) if not keep_dead_letter else received.body

        transport.publish(queue, body)  # re-publish to the source queue first,
        transport.ack(received)         # then remove from the DLQ (no message lost)
        moved += 1

        trace = envelope.get("trace_id")
        urn = envelope.get("job") or envelope.get("urn")
        print(f"[redrive] {urn}  trace={trace}  {dlq} -> {queue}")

    print(f"[redrive] moved {moved} message(s) from '{dlq}' back to '{queue}'.")
    return moved


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-drive BabelQueue DLQ messages back to the source queue.")
    parser.add_argument("--queue", default=QUEUE, help="source queue name (DLQ is <queue>.dlq)")
    parser.add_argument("--max", type=int, default=None, help="re-drive at most N messages (default: all)")
    parser.add_argument(
        "--keep-dead-letter",
        action="store_true",
        help="re-drive the envelope untouched (keep the dead_letter block and attempts)",
    )
    args = parser.parse_args()

    redrive(
        BROKER_URL,
        args.queue,
        max_messages=args.max,
        keep_dead_letter=args.keep_dead_letter,
    )


if __name__ == "__main__":
    main()
