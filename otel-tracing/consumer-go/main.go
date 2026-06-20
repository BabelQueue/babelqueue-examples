// Consumer (Go) — handles an order inside a CONSUMER span that lands in the SAME OTel trace as
// the Python producer's PRODUCER span (ADR-0025). otel.WrapHandler(tracer, handler) emits a
// "process <urn>" span and derives its trace from the envelope's trace_id, via the trace_id ↔
// OTel TraceID bijection — so a message a Python (or PHP/Java/Node) producer stamped with a
// trace_id is observed here under that exact trace, no broker-side context propagation needed.
//
//	go run .
//
// A console span exporter (stdouttrace) prints the CONSUMER span to stdout, so the demo needs
// no OTel collector. Compare the printed span's TraceID with the trace_id the producer logged:
// they are the same trace. The producer (../producer-python/produce.py) publishes the order.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqotel "github.com/babelqueue/babelqueue-go/otel"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const urn = "urn:babel:orders:created"

func main() {
	url := os.Getenv("BROKER_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	queue := os.Getenv("QUEUE")
	if queue == "" {
		queue = "orders"
	}

	// Wire a TracerProvider with a console exporter so spans print to stdout (self-contained
	// demo). stdouttrace.WithPrettyPrint makes the TraceID easy to eyeball against the producer.
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		panic(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("orders-consumer")

	transport, err := bqredis.New(url)
	if err != nil {
		panic(err)
	}
	defer transport.Close()

	app := babelqueue.NewApp(transport, babelqueue.WithDefaultQueue(queue))

	// The business handler — runs inside the CONSUMER span otel.WrapHandler opens for it.
	handle := func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] processed   order_id=%v amount=%v %v  meta.id=%s  trace_id=%s\n",
			env.Data["order_id"], env.Data["amount"], env.Data["currency"], env.Meta.ID, env.TraceID)
		return nil
	}

	// otel.WrapHandler derives the consume span's trace from env.TraceID (the bijection), so the
	// span lands in the producer's trace. Register it like any handler.
	app.Handle(urn, bqotel.WrapHandler(tracer, handle))

	// Drain everything currently queued, then stop. For a long-running worker use app.Consume.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	processed, err := app.Drain(ctx, queue, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[go] handled %d message(s) — each CONSUMER span shares the producer's trace via trace_id.\n", processed)
}
