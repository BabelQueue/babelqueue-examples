# Redis orders — Python *or* PHP produces, Go *or* Java consumes

A producer publishes canonical BabelQueue envelopes to a Redis queue; a **Go** or
**Java** service reads the *same* queue and routes by URN. Same wire envelope,
different languages, one broker — no PHP `serialize()`, no language-specific
format. The producer can be **Python** or **PHP** (or any SDK) — the consumer
doesn't know or care which one wrote the message; it only sees the canonical
envelope and `meta.lang`.

Every SDK uses the identical reliable-queue pattern (`RPUSH` to produce, `BLMOVE`
to a `:processing` list to reserve, `LREM` to ack), so they interoperate on the
plain `orders` Redis list.

## Run it

```bash
# 1) start Redis
docker compose up -d            # or: docker run -d -p 6379:6379 redis:7
```

Then run **one** producer:

```bash
# 2a) producer — Python
cd producer-python
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python produce.py
cd ..
```

```bash
# 2b) producer — PHP   (needs babelqueue/php-sdk ^1.0, which ships RedisTransport)
cd producer-php
composer install
php produce.php
cd ..
```

Then run **one** consumer:

```bash
# 3a) consumer — Go
cd consumer-go
go run .
```

Expected consumer output — a Go program reading messages it never produced. Note
**`produced by "php"`** (or `"python"` if you ran that producer): the data,
`trace_id` and unicode all survive the language boundary.

```
[go] order created  id=1042 amount=99.9 USD  trace=…  (produced by "php")
[go] order created  id=1043 amount=12.5 EUR  trace=…  (produced by "php")
[go] item indexed   sku=WIDGET-1 title="Café Widget ☕"  (produced by "php")
[go] processed 3 message(s) — same envelope, different language.
```

```bash
# 3b) consumer — Java   (needs babelqueue-redis 1.0.0 — once it lands on Maven
#     Central this resolves automatically; before then, build it into ~/.m2 with
#     `mvn -f ../../../babelqueue-java/pom.xml install -DskipTests` then
#     `mvn -f ../../../babelqueue-java-redis/pom.xml install -DskipTests`)
cd consumer-java
mvn -q compile exec:java          # REDIS_URL env-configurable (default redis://localhost:6379/0)
```

## Proven: PHP produces → Java consumes (live, over a real Redis)

The Java consumer (`consumer-java/`) reserves with `BLMOVE orders orders:processing`
and acks with `LREM` — the identical §1 reliable-queue convention the PHP producer's
`RPUSH` feeds. Running the PHP producer (`producer-php/produce-shipped.php`, 3×
`orders:created` + 1× `orders:shipped`, `lang=php`) and then `mvn -q compile exec:java`
captured this — a **Java** program reading messages a **PHP** SDK wrote, routed by URN,
with the producer's `lang`, intact data (note the unicode carrier) and preserved
`trace_id` surviving the language boundary:

```
[java] orders:created  order_id=1042  amount=99.9 USD  from lang=php  trace=dd3e7352-8242-4dc5-8d8f-e9163f185cc9
[java] orders:created  order_id=1043  amount=12.5 EUR  from lang=php  trace=3c046f11-868b-432c-bc90-62d909584a93
[java] orders:created  order_id=1044  amount=7.25 GBP  from lang=php  trace=e3fb4277-4aca-4844-9c10-c43d93171c58
[java] orders:shipped  order_id=1042  carrier=DHL Express ✈  from lang=php  trace=b9cf2f99-9ba9-4206-81a8-44611c7f97d8
[java] processed 4 message(s) — same envelope, different language.
```

After the run both `orders` and `orders:processing` are empty (`LLEN` = 0): every
message was reserved and acked. The Redis list element **is** the raw canonical
envelope — no wrapping, no PHP `serialize()`.

## Swap the ends

The queue carries the canonical envelope, so any SDK can be on either side:

- **PHP producer:** `EnvelopeCodec::fromJob(...)` + `RedisTransport::publish(...)`
  (framework-less core), or Laravel/Symfony on a `babelqueue-*` connection.
- **Go producer:** `babelqueue.Make(...)` + `transport.Publish(...)`, or build the
  same `App` and call `app.Publish(...)`.
- **Python consumer:** `@app.handler("urn:...")` + `app.run()`.
- **Java consumer:** `RedisConsumer.builder(redis, "orders").handler("urn:...", h).poll()`
  / `.run()` (Lettuce), as in `consumer-java/` above.

Node and .NET read/write the identical envelope on their own framework
transports — see [babelqueue.com](https://babelqueue.com).
