"""DLQ re-drive (Python) — moves canonical envelopes from ``<queue>.dlq`` back to
the source ``<queue>`` so they get another run, after you have fixed whatever made
them fail. This is the cross-language re-drive case: the DLQ holds plain canonical
envelopes, so the message a *Go* or *PHP* consumer dead-lettered is re-driven here
and can be picked up by a consumer in *any* language.

    pip install "babelqueue[redis]"
    python redrive.py                 # re-drive every message on orders.dlq
    python redrive.py --max 1         # re-drive at most one
    python redrive.py --keep-dead-letter   # leave the dead_letter block in place
    python redrive.py --bypass        # replay, but skip side-effects that already fired

Each message is reserved on the DLQ (``pop``), re-published to the source queue,
then acked off the DLQ — so an interrupted run never loses or duplicates messages
beyond the at-least-once guarantee. By default the additive ``dead_letter`` block
is stripped and ``attempts`` reset to 0, giving the message a clean re-run; the
original identity (``trace_id`` / ``meta.id`` / ``data``) is preserved verbatim.
Run only after the underlying fault is fixed, or the message will just fail again.

``--bypass`` stamps the out-of-band ``bq-replay-bypass`` transport header
(:data:`babelqueue.HEADER_REPLAY_BYPASS`, ADR-0027) on each redriven message, so a
handler can re-run its idempotent core but **skip** an external side-effect that
already fired on the original run — don't re-charge, don't re-email. The marker
rides out of band, so the frozen envelope is untouched (GR-1). It only propagates
over a transport that implements the optional :class:`~babelqueue.transport.HeaderPublisher`
capability; today the in-memory transport does (see ``replay_bypass_demo.py`` for the
end-to-end, broker-free demo), while the Redis transport does not yet — so over Redis
``--bypass`` falls back to a plain publish and prints a notice. Wiring the header onto
the broker transports is a follow-up, like the broker bindings themselves.
"""

import argparse
import os

from babelqueue import HEADER_REPLAY_BYPASS, EnvelopeCodec
from babelqueue.transport import HeaderPublisher, make_transport

BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("QUEUE", "orders")


def redrive(
    broker_url: str, queue: str, *, max_messages=None, keep_dead_letter=False, bypass=False
) -> int:
    """Move messages from ``<queue>.dlq`` back to ``<queue>``; return the count."""
    dlq = f"{queue}.dlq"
    transport = make_transport(broker_url)
    can_bypass = isinstance(transport, HeaderPublisher)
    if bypass and not can_bypass:
        print(
            f"[redrive] note: {type(transport).__name__} does not carry transport headers yet, "
            f"so --bypass is a no-op here (the replay re-fires side-effects). "
            f"See replay_bypass_demo.py for the in-memory end-to-end demo."
        )
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

        if bypass and can_bypass:
            # Stamp the replay marker so the handler skips already-done external effects.
            transport.publish_with_headers(queue, body, {HEADER_REPLAY_BYPASS: "1"})
        else:
            transport.publish(queue, body)  # re-publish to the source queue first,
        transport.ack(received)             # then remove from the DLQ (no message lost)
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
    parser.add_argument(
        "--bypass",
        action="store_true",
        help="stamp the bq-replay-bypass marker so handlers skip already-done external effects",
    )
    args = parser.parse_args()

    redrive(
        BROKER_URL,
        args.queue,
        max_messages=args.max,
        keep_dead_letter=args.keep_dead_letter,
        bypass=args.bypass,
    )


if __name__ == "__main__":
    main()
