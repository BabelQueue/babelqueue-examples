# Redis orders — Python *or* PHP produces, Go consumes

A producer publishes canonical BabelQueue envelopes to a Redis queue; a **Go**
service reads the *same* queue and routes by URN. Same wire envelope, different
languages, one broker — no PHP `serialize()`, no language-specific format. The
producer can be **Python** or **PHP** (or any SDK) — the Go consumer doesn't know
or care which one wrote the message; it only sees the canonical envelope and
`meta.lang`.

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
# 2b) producer — PHP   (needs babelqueue/php-sdk >= 0.3, which ships RedisTransport)
cd producer-php
composer install
php produce.php
cd ..
```

```bash
# 3) consumer — Go
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

## Swap the ends

The queue carries the canonical envelope, so any SDK can be on either side:

- **PHP producer:** `EnvelopeCodec::fromJob(...)` + `RedisTransport::publish(...)`
  (framework-less core), or Laravel/Symfony on a `babelqueue-*` connection.
- **Go producer:** `babelqueue.Make(...)` + `transport.Publish(...)`, or build the
  same `App` and call `app.Publish(...)`.
- **Python consumer:** `@app.handler("urn:...")` + `app.run()`.

Node, Java and .NET read/write the identical envelope on their own framework
transports — see [babelqueue.com](https://babelqueue.com).
