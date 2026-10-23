#!/usr/bin/env python3
"""Kafka cross-language retry probe (Python side), R1 Faz 10 scenario 5.

Environment: KAFKA_BROKERS, TOPIC (work topic), TIER_MS (retry.1 delay), GROUP (consumer group of
the Java failing consumer; the Python consumer joins the SAME group so it only sees the record the
re-injector writes after the original one was committed), TIMEOUT (seconds, exit 3 on timeout).

Modes (first argument):
  produce  publish one envelope with a record key and a W3C ``traceparent`` header
  consume  pop the re-injected record through KafkaTransport, then ack
  check    read the work topic and <TOPIC>.retry.1 from the beginning and assert the §6 retry
           invariants (5c byte-identity, A6 wait, A7 header rules, attempts +1);
           TRACEPARENT (env) is the value printed by `produce`

Output lines (``PROBE python <EVENT> name=value ...``) follow the redis-orders probe contract;
``check`` prints ``KAFKA CHECK ...`` lines and exits 1 when any assertion fails.
"""
from __future__ import annotations

import os
import sys
import time
import uuid

from babelqueue import EnvelopeCodec
from babelqueue.kafka_transport import KafkaTransport

URN = os.environ.get("URN", "urn:babel:orders:created")
BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "orders")
GROUP = os.environ.get("GROUP", "bq-interop-" + TOPIC)
TIER_MS = int(os.environ.get("TIER_MS", "10000"))
TIMEOUT_S = float(os.environ.get("TIMEOUT", "60"))
KEY = b"order-42"
# `produce` generates a fresh W3C traceparent and prints it; the runner hands that value back to `check`
# through this variable. A per-process random value would make `check` compare against its own value.
TRACEPARENT = os.environ.get("TRACEPARENT") or "00-" + uuid.uuid4().hex + "-" + uuid.uuid4().hex[:16] + "-01"
RETRY_TOPIC = TOPIC + ".retry.1"


def now_ms() -> int:
    return int(time.time() * 1000)


def say(event: str, **fields) -> None:
    parts = ["PROBE python " + event] + ["%s=%s" % (k, v) for k, v in fields.items()]
    print(" ".join(parts), flush=True)


class _KeyedProducer:
    """Wraps a confluent Producer so KafkaTransport.publish writes a record key and a traceparent
    header: the transport itself publishes neither, and the scenario needs both to check that the
    retry/re-inject path carries them through unchanged."""

    def __init__(self, inner, traceparent: str, key: bytes) -> None:
        self._inner = inner
        self._traceparent = traceparent
        self._key = key

    def produce(self, topic, **kwargs):
        headers = list(kwargs.get("headers") or [])
        headers.append(("traceparent", self._traceparent.encode()))
        kwargs["headers"] = headers
        kwargs.setdefault("key", self._key)
        return self._inner.produce(topic, **kwargs)

    def poll(self, *args, **kwargs):
        return self._inner.poll(*args, **kwargs)

    def flush(self, *args, **kwargs):
        return self._inner.flush(*args, **kwargs)


def do_produce() -> int:
    from confluent_kafka import Producer

    inner = Producer({"bootstrap.servers": BROKERS, "acks": "all"})
    transport = KafkaTransport(url="kafka://" + BROKERS, producer=_KeyedProducer(inner, TRACEPARENT, KEY))
    envelope = EnvelopeCodec.make(
        URN,
        {"order_id": 42, "total": 99.5, "currency": "TRY"},
        queue=TOPIC,
    )
    transport.publish(TOPIC, EnvelopeCodec.encode(envelope))
    inner.flush(30)
    say("PUBLISHED", id=envelope["meta"]["id"], key=KEY.decode(), traceparent=TRACEPARENT, at_ms=now_ms())
    return 0


def do_consume() -> int:
    transport = KafkaTransport(url="kafka://" + BROKERS, group_id=GROUP)
    say("READY")
    deadline = time.time() + TIMEOUT_S
    try:
        while time.time() < deadline:
            message = transport.pop(TOPIC, 1.0)
            if message is None:
                continue
            decoded = EnvelopeCodec.decode(message.body)
            envelope_id = decoded["meta"]["id"]
            say("RECEIVED", id=envelope_id, attempts=decoded.get("attempts", 0), at_ms=now_ms())
            transport.ack(message)
            say("ACKED", id=envelope_id)
            return 0
        say("TIMEOUT")
        return 3
    finally:
        transport.close()


def _headers(record) -> dict:
    return {name: (value if value is not None else b"") for name, value in (record.headers() or [])}


def _attempts_of(record) -> tuple:
    import json

    body = json.loads(record.value().decode("utf-8"))
    header = _headers(record).get("bq-attempts")
    return body.get("attempts"), (int(header) if header is not None else None)


def do_check() -> int:
    from confluent_kafka import Consumer, TopicPartition  # noqa: F401

    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": "bq-interop-check-" + uuid.uuid4().hex,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC, RETRY_TOPIC])
    original = retry = reinjected = None
    deadline = time.time() + 40
    while time.time() < deadline and not (original and retry and reinjected):
        message = consumer.poll(1.0)
        if message is None or message.error():
            continue
        if message.topic() == RETRY_TOPIC:
            retry = retry or message
        elif original is None:
            original = message
        elif reinjected is None:
            reinjected = message
    consumer.close()

    failures = []

    def check(label: str, ok: bool, detail: str = "") -> None:
        print("KAFKA CHECK %s: %s%s" % (label, "OK" if ok else "FAIL", (" (" + detail + ")") if detail else ""), flush=True)
        if not ok:
            failures.append(label)

    check("records present (original, retry.1, reinjected)", bool(original and retry and reinjected))
    if failures:
        print("KAFKA CHECK FAILED: %s" % ", ".join(failures), flush=True)
        return 1

    o_body, o_hdr = _attempts_of(original)
    r_body, r_hdr = _attempts_of(retry)
    j_body, j_hdr = _attempts_of(reinjected)
    check(
        "attempts body+header both +1 (original -> retry.1)",
        (o_body, o_hdr, r_body, r_hdr) == (0, 0, 1, 1),
        "original body=%s hdr=%s, retry body=%s hdr=%s" % (o_body, o_hdr, r_body, r_hdr),
    )
    check(
        "attempts preserved on reinjected (== retry.1)",
        (j_body, j_hdr) == (r_body, r_hdr),
        "reinjected body=%s hdr=%s" % (j_body, j_hdr),
    )
    check("value bytes identical (retry.1 vs reinjected)", retry.value() == reinjected.value())
    check(
        "key bytes identical (original vs retry.1 vs reinjected)",
        original.key() == retry.key() == reinjected.key() == KEY,
        "key=%r" % (reinjected.key(),),
    )
    r_headers, j_headers = _headers(retry), _headers(reinjected)
    check(
        "retry.1 carries bq-delay and bq-original-topic",
        r_headers.get("bq-delay") == str(TIER_MS).encode() and r_headers.get("bq-original-topic") == TOPIC.encode(),
        "bq-delay=%r bq-original-topic=%r" % (r_headers.get("bq-delay"), r_headers.get("bq-original-topic")),
    )
    check(
        "no bq-delay / bq-original-topic on reinjected",
        "bq-delay" not in j_headers and "bq-original-topic" not in j_headers,
    )
    o_headers = _headers(original)
    # TRACEPARENT in the environment is the value `produce` printed; without it the original record's
    # header is the reference. Either way it must be non-empty and survive both hops unchanged.
    expected_tp = (os.environ.get("TRACEPARENT") or "").encode() or o_headers.get("traceparent") or b""
    check(
        "traceparent preserved (original, retry.1, reinjected)",
        bool(expected_tp)
        and o_headers.get("traceparent") == r_headers.get("traceparent") == j_headers.get("traceparent") == expected_tp,
        "expected=%r original=%r retry.1=%r reinjected=%r"
        % (expected_tp, o_headers.get("traceparent"), r_headers.get("traceparent"), j_headers.get("traceparent")),
    )
    wait_ms = reinjected.timestamp()[1] - retry.timestamp()[1]
    # A6: the lower bound is strict (200 ms of timestamp/clock skew). A re-injector that ignores
    # bq-delay forwards within a poll interval and fails it; the upper bound only absorbs poll latency.
    check(
        "wait from retry.1 timestamp >= tier and <= tier + 8s (A6)",
        TIER_MS - 200 <= wait_ms <= TIER_MS + 8000,
        "wait=%d ms, tier=%d ms" % (wait_ms, TIER_MS),
    )
    print("KAFKA CHECK wait=%d ms (tier=%d ms)" % (wait_ms, TIER_MS), flush=True)
    if failures:
        print("KAFKA CHECK FAILED: %s" % ", ".join(failures), flush=True)
        return 1
    print("KAFKA CHECK ALL OK", flush=True)
    return 0


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "produce":
        return do_produce()
    if mode == "consume":
        return do_consume()
    if mode == "check":
        return do_check()
    print("usage: kafka_probe.py produce|consume|check", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
