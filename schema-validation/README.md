# Per-URN schema validation — reject bad data before it enters the queue

The envelope frame is strict, but its `data` block is **open JSON** — by design, so
every domain can carry its own payload. Per-URN schema validation
([ADR-0024](https://babelqueue.com)) closes that gap **opt-in**: you register a
JSON Schema (draft-07) for a message URN, and BabelQueue validates the `data`
against it. A URN with no registered schema is never validated, so adoption is
incremental and the wire envelope stays frozen at `schema_version: 1`.

The schema lives in a **babelqueue-registry** manifest (`registry.json`), the
same artifact the registry tooling governs — so the schema enforced at runtime is
exactly the one the registry owns. This example ships a tiny registry for one URN
and validates against it at **both** ends:

1. A **producer-side guard** (preferred): `schema.Check(provider, urn, data)`
   validates before publishing, so invalid data **never enters the queue**. The
   producer sends one valid order (accepted, published) and tries one invalid
   order (rejected before send, never published).
2. A **consumer-side safety net**: `schema.Wrap(provider, handler)` validates each
   message before the handler runs — defence in depth against a message some
   *other*, unvalidated producer slipped onto the queue. Invalid data takes the
   retry / dead-letter path instead of reaching business logic.

Both ends read the same `registry.json` via a `DirProvider`, so one governed
schema is enforced everywhere. Everything runs on the simplest broker, Redis (§1).

> Validation is **opt-in and additive**: only URNs you register a schema for are
> checked, and the validator covers the JSON-Schema subset whose verdicts match
> across the Go / Python / PHP validators and babelqueue-registry's `compat`
> linter (`type`, `required`, `properties`, `additionalProperties`, `items`,
> `enum`, `const`, `minLength`, `minimum`). Unknown keywords are ignored.

## The registry

[`registry.json`](registry.json) maps the URN to its schema file — the
babelqueue-registry manifest shape (`{urn, schema, owner, status}`):

```json
{
  "schemas": [
    { "urn": "urn:babel:orders:created", "schema": "schemas/orders-created.json",
      "owner": "orders", "status": "active" }
  ]
}
```

[`schemas/orders-created.json`](schemas/orders-created.json) is the draft-07
schema for that URN's `data`: `order_id` (integer ≥ 1) and `amount` (number ≥ 0)
are **required**, `currency` must be one of `USD` / `EUR` / `TRY`, and no extra
keys are allowed (`additionalProperties: false`).

## Run it

```bash
# 1) start Redis
docker compose up -d            # or: docker run -d -p 6379:6379 redis:7
```

```bash
# 2) producer — Go   (validates each order, then publishes only the valid one)
#    needs babelqueue-go ^1.5; the schema package is in the core module — no extra
#    `go get`; the Redis transport is …/redis
cd producer-go
go run .
cd ..
```

Expected producer output — the valid order is published; the invalid one is
**rejected before send**, so it never reaches the queue:

```
[go] PUBLISHED  order_id=1042  meta.id=…  (valid order)
[go] REJECTED   order_id=1043  (amount < 0 and currency not in enum)
             babelqueue/schema: message data does not match its URN schema for "urn:babel:orders:created": amount: below_minimum; currency: not_in_enum
[go] validated 2 order(s) against "urn:babel:orders:created"'s schema — published 1, rejected 1 before send.
```

```bash
# 3) consumer — Go   (the safety net: validates again before the handler runs)
cd consumer-go
go run .
```

Because the producer already kept invalid data out, the consumer sees only the
clean order — and the `schema.Wrap` safety net confirms it before the handler runs:

```
[go] processed   order_id=1042 amount=99.9 EUR  meta.id=…  (data validated)
[go] handled 1 message(s) — each validated against its URN schema before the handler ran.
```

## How the guard works

`schema.Check(provider, urn, data)` is the **producer-side** guard:

- looks up the schema for `urn` in the `Provider`;
- **no schema registered** → returns `nil` (validation is opt-in — the message
  publishes unchanged);
- **valid** → returns `nil`;
- **invalid** → returns an error wrapping `schema.ErrInvalidPayload` (detect with
  `errors.Is`), listing the violations — so you skip the publish.

`schema.Wrap(provider, handler)` is the **consumer-side** safety net: it validates
`env.Data` against the message's URN schema first, and only then calls the handler.
Invalid data returns `ErrInvalidPayload`, so the runtime retries and eventually
**dead-letters** the poison message — it will never become valid on retry, which is
exactly why producer-side `Check` is preferred (keep it out of the queue entirely).

A provider **lookup** error (e.g. the registry file is briefly unavailable during a
deploy) is distinct from invalid data: it is returned as a plain error so the
operation is retried once the source recovers, not treated as a bad payload.

## Configuration

All scripts read these environment variables:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `BROKER_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `QUEUE` | `orders` | queue the order envelopes flow over |
| `REGISTRY` | `../registry.json` | babelqueue-registry manifest the schemas come from |

## Swap the ends

The schema is keyed on the canonical URN and the registry is plain JSON, so any
SDK can validate against it — same verdicts, same registry:

- **Python:** `from babelqueue.schema import DirProvider, check, wrap`, then
  `check(provider, urn, data)` producer-side or
  `app.register(urn, wrap(provider, urn, handler))` consumer-side
  (`DirProvider("registry.json")`, or `MapProvider.from_json({...})` for embedded
  schemas).
- **PHP:** `BabelQueue\Schema\SchemaValidated::assert` / `::check` / `::wrap` with a
  `DirProvider('registry.json')`.
- **Go (in-memory):** `schema.NewMapProvider(map[string][]byte{...})` instead of a
  `DirProvider` when you embed schemas in code rather than read the registry.

See [babelqueue.com](https://babelqueue.com) for the per-SDK schema APIs.
