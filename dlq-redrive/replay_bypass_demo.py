"""Replay-Bypass demo (Python) — redrive a dead-lettered message, but DON'T re-fire
the side-effects that already happened on the original run (ADR-0027).

Idempotency (see ../idempotency-payments) stops an *accidental* duplicate. This is the
other half: a *deliberate* replay off the DLQ re-runs the handler, so without a guard its
external effects re-fire — a second charge, a duplicate email. The Replay-Bypass guard lets
the handler re-run its idempotent core while skipping effects that already fired::

    from babelqueue import is_replay, bypass_external_effects

    @app.handler("urn:babel:orders:created")
    def on_order_created(data, meta):
        save_order(data)                                   # idempotent core — always runs
        bypass_external_effects(lambda: send_email(data))  # external effect — skipped on replay

``redrive(..., bypass=True)`` stamps the out-of-band ``bq-replay-bypass`` transport header on
each redriven message; the runtime surfaces it to the handler as :func:`is_replay`, and
:func:`bypass_external_effects` runs its callable only when this is NOT a replay. The marker
rides out of band, so the frozen envelope is untouched (GR-1).

This script runs the WHOLE loop in one process on the in-memory transport (``memory://``),
which implements the optional :class:`~babelqueue.transport.HeaderPublisher` capability — so the
marker propagates end-to-end with no broker. It first replays WITHOUT bypass (the email
re-fires) and then WITH bypass (the email is skipped), so you can see the difference:

    pip install babelqueue        # no broker extra needed — runs on memory://
    python replay_bypass_demo.py

Over a real broker the ``bq-replay-bypass`` header propagates only once that broker's
transport implements ``HeaderPublisher`` (a follow-up, like the broker bindings); the Redis
``redrive.py --bypass`` therefore prints a no-op notice today. This demo proves the contract.
"""

from babelqueue import (
    BabelQueue,
    EnvelopeCodec,
    bypass_external_effects,
    dead_letter,
)
from babelqueue.redrive import redrive

URN = "urn:babel:orders:created"
QUEUE = "orders"
DLQ = f"{QUEUE}.dlq"

# Tracks the external side-effect — the thing we must NOT do twice.
sent_emails: list[int] = []


def build_app() -> BabelQueue:
    """A fresh in-memory app whose handler runs an idempotent core + one external effect."""
    app = BabelQueue("memory://", queue=QUEUE)

    @app.handler(URN)
    def on_order_created(data, meta):
        order_id = data["order_id"]
        # Idempotent core — safe to re-run on every delivery (always runs).
        print(f"[handler] processed order {order_id} (idempotent core — always runs)")
        # External, non-idempotent effect — guard it so a replay does not re-fire it.
        bypass_external_effects(lambda: send_confirmation_email(order_id))

    return app


def send_confirmation_email(order_id: int) -> None:
    """Stands in for a real, non-idempotent side-effect (charge a card, call a 3rd party)."""
    sent_emails.append(order_id)
    print(f"[handler]   -> sent confirmation email for order {order_id}")


def seed_dead_letter(app: BabelQueue, order_id: int) -> None:
    """Put a dead-lettered envelope on the DLQ — as if a worker quarantined it AFTER it had
    already sent the email on the original run (the effect we must not repeat)."""
    envelope = EnvelopeCodec.make(URN, {"order_id": order_id}, queue=QUEUE)
    envelope["attempts"] = 3
    annotated = dead_letter.annotate(
        envelope, "failed", QUEUE, 3, error="downstream 503 after the email went out", exception="HttpError"
    )
    app.transport.publish(DLQ, EnvelopeCodec.encode(annotated))
    print(f"[seed] dead-lettered order {order_id} on '{DLQ}' (email had already gone out)")


def run(label: str, *, bypass: bool) -> None:
    sent_emails.clear()
    print(f"\n=== {label} (bypass={bypass}) ===")
    app = build_app()
    seed_dead_letter(app, order_id=1042)

    result = redrive(app.transport, DLQ, bypass=bypass)
    stamped = result.items[0].bypassed if result.items else False
    print(f"[redrive] redriven={result.redriven}  bq-replay-bypass stamped={stamped}  {DLQ} -> {QUEUE}")

    processed = app.consume(QUEUE, max_messages=10, timeout=0.1)
    print(f"[consume] handled {processed} message(s); emails sent on the replay: {sent_emails}")


def main() -> None:
    # Same fault-fixed message, two replays — only the guard differs.
    run("Plain redrive — the email RE-FIRES", bypass=False)
    run("Redrive WITH replay-bypass — the email is SKIPPED", bypass=True)

    print(
        "\nReplay-Bypass lets you safely re-drive a fixed message without re-charging or "
        "re-emailing: the idempotent core ran both times, but the external effect fired only "
        "on the original run, not on the bypassed replay."
    )


if __name__ == "__main__":
    main()
