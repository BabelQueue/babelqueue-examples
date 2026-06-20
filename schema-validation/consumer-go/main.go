// Consumer (Go) — the consumer-side safety net. schema.Wrap(provider, handler) validates each
// message's data against its URN schema BEFORE the handler runs (ADR-0024): valid data (or a
// URN with no registered schema) runs the handler unchanged; invalid data returns
// schema.ErrInvalidPayload, so the runtime retries and eventually dead-letters the poison
// message instead of feeding it to business logic.
//
//	go run .
//
// The producer (../producer-go) already rejects invalid data producer-side, so in the happy
// path this consumer only ever sees clean orders — the wrap is defence in depth against a
// message that some *other*, unvalidated producer slipped onto the queue. It reads the same
// ../registry.json the producer validated against, so both ends enforce one governed schema.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
	"github.com/babelqueue/babelqueue-go/schema"
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
	registry := os.Getenv("REGISTRY")
	if registry == "" {
		registry = filepath.Join("..", "registry.json")
	}

	transport, err := bqredis.New(url)
	if err != nil {
		panic(err)
	}
	defer transport.Close()

	app := babelqueue.NewApp(transport, babelqueue.WithDefaultQueue(queue))

	provider, err := schema.NewDirProvider(registry)
	if err != nil {
		panic(err)
	}

	// The business handler — only ever runs on data that already passed validation.
	handle := func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] processed   order_id=%v amount=%v %v  meta.id=%s  (data validated)\n",
			env.Data["order_id"], env.Data["amount"], env.Data["currency"], env.Meta.ID)
		return nil
	}

	// schema.Wrap is the safety net: it validates env.Data against urn's schema first and only
	// then calls handle. Register it like any handler.
	app.Handle(urn, schema.Wrap(provider, handle))

	// Drain everything currently queued, then stop. For a long-running worker use app.Consume.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	processed, err := app.Drain(ctx, queue, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[go] handled %d message(s) — each validated against its URN schema before the handler ran.\n", processed)
}
