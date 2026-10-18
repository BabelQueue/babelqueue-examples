# Azure Service Bus orders — Python produces, Go consumes

A **Python** producer publishes canonical BabelQueue envelopes to an Azure Service Bus
queue; a **Go** service reads the *same* queue and routes by URN. Same wire envelope,
different languages, one broker — no PHP `serialize()`, no language-specific format. The Go
consumer never knows which language wrote the message; it only sees the canonical envelope
and `meta.lang`.

Each SDK uses the [§4 Azure Service Bus binding](https://babelqueue.com/docs/spec/1.x/broker-bindings):
the envelope is the AMQP message body, projected onto native `ApplicationProperties`
(`bq-job` = URN, `bq-trace-id`, `bq-message-id`, …) so a consumer can route and trace
without decoding the body.

> There is no PHP producer here on purpose: Azure Service Bus has no conformant PHP path
> (it is AMQP 1.0 only — see ADR-0021). Python, Go, Node, Java and .NET all speak §4.

The demo runs against the official **Azure Service Bus emulator**
(`mcr.microsoft.com/azure-messaging/servicebus-emulator`, backed by
`mcr.microsoft.com/mssql/server`, SQL Server 2022), so it needs no Azure subscription. `Config.json`
pre-creates the `orders` queue. Running the emulator means you accept the Microsoft EULA
(`ACCEPT_EULA=Y` in `docker-compose.yml`).

## Run it

```bash
# 1) start the emulator (AMQP on 5672, health endpoint on 5300). The first start takes
#    up to a minute while SQL Server comes up; wait until the emulator logs that it is ready.
docker compose up -d
docker compose logs -f emulator
```

```bash
# 2) producer — Python   (needs babelqueue[azureservicebus], which ships AsbTransport)
cd producer-python
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python produce.py
cd ..
```

```bash
# 3) consumer — Go   (needs babelqueue-go/azureservicebus)
cd consumer-go
go run .
```

The producer and consumer default to the emulator's development connection string
(`Endpoint=sb://localhost;…;UseDevelopmentEmulator=true;`), so the commands above work as-is.

Expected consumer output — a Go program reading messages it never produced. Note
**`produced by "python"`**: the data, `trace_id` and unicode all survive the language
boundary.

```
[go] order created  id=1042 amount=99.9 USD  trace=…  (produced by "python")
[go] order created  id=1043 amount=12.5 EUR  trace=…  (produced by "python")
[go] item indexed   sku=WIDGET-1 title="Café Widget ☕"  (produced by "python")
[go] processed 3 message(s) — same envelope, different language.
```

## Point it at real Azure Service Bus

```bash
export ASB_CONNECTION_STRING='Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=…;SharedAccessKey=…'
# create an `orders` queue in that namespace, then run the producer + consumer as above.
```

## Swap the ends

The queue carries the canonical envelope, so any SDK can be on either side:

- **Go producer:** `azureservicebus.New(…)` + `app.Publish(...)`.
- **Python consumer:** `@app.handler("urn:...")` + `app.run()` on an `sb://` URL.
- **Node / Java / .NET** read/write the identical envelope on their own Service Bus
  transports — see [babelqueue.com](https://babelqueue.com).

## Cleanup

```bash
docker compose down
```
