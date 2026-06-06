# Redis orders — Python produces, Go consumes

A **Python** service publishes canonical BabelQueue envelopes to a Redis queue;
a **Go** service reads the *same* queue and routes by URN. Same wire envelope,
two languages, one broker — no PHP `serialize()`, no language-specific format.

Both SDKs use the identical reliable-queue pattern (`RPUSH` to produce, `BLMOVE`
to a `:processing` list to reserve, `LREM` to ack), so they interoperate on the
plain `orders` Redis list.

## Run it

```bash
# 1) start Redis
docker compose up -d            # or: docker run -d -p 6379:6379 redis:7

# 2) producer (Python)
cd producer-python
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python produce.py
cd ..

# 3) consumer (Go)
cd consumer-go
go run .
```

Expected consumer output (note **produced by "python"** — a Go program reading
Python-produced messages):

```
[go] order created  id=1042 amount=99.9 USD  trace=…  (produced by "python")
[go] order created  id=1043 amount=12.5 EUR  trace=…  (produced by "python")
[go] item indexed   sku=WIDGET-1 title="Café Widget ☕"  (produced by "python")
[go] processed 3 message(s) — same envelope, different language.
```

## Swap the ends

The queue carries the canonical envelope, so any SDK can be on either side:

- **Go producer:** `babelqueue.Make(...)` + `transport.Publish(...)`, or build the
  same `App` and call `app.Publish(...)`.
- **Python consumer:** `@app.handler("urn:...")` + `app.run()`.

PHP/Node/Java/.NET read/write the same envelope; a matching plain-Redis transport
for those is on the roadmap (today they ship the codec + their own framework
transports — see [babelqueue.com](https://babelqueue.com)).
