// Producer (Go) — validates each order's data against the URN's registered JSON Schema
// BEFORE publishing, so invalid data never enters the queue (ADR-0024). schema.Check is the
// producer-side guard: it looks up the schema for the message URN in a Provider and returns
// schema.ErrInvalidPayload (wrapped) when the data does not match — call it before app.Publish.
//
//	go run .
//
// The Provider here is a schema.DirProvider reading ../registry.json, the same
// babelqueue-registry manifest the bqschema tooling governs — so the schema enforced at
// runtime is exactly the one the registry owns. A URN with no registered schema is never
// validated (validation is opt-in).
//
// The demo sends ONE valid order (accepted, published) and tries ONE invalid order (rejected
// before send — never published), so the consumer downstream only ever sees clean data.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
		// Default to the manifest shipped alongside this example (../registry.json).
		registry = filepath.Join("..", "registry.json")
	}

	transport, err := bqredis.New(url)
	if err != nil {
		panic(err)
	}
	defer transport.Close()

	app := babelqueue.NewApp(transport, babelqueue.WithDefaultQueue(queue))

	// DirProvider bridges the babelqueue-registry manifest to runtime validation: it maps each
	// URN to its draft-07 schema file and parses/caches it on first use. NewMapProvider is the
	// in-memory alternative for tests / embedded schemas.
	provider, err := schema.NewDirProvider(registry)
	if err != nil {
		panic(err)
	}

	// (data, why) — the first is valid; the second violates the schema (negative amount,
	// unknown currency) so the guard must reject it before it ever reaches the queue.
	orders := []struct {
		data map[string]any
		note string
	}{
		{map[string]any{"order_id": 1042, "amount": 99.90, "currency": "EUR"}, "valid order"},
		{map[string]any{"order_id": 1043, "amount": -5.00, "currency": "BTC"}, "amount < 0 and currency not in enum"},
	}

	published := 0
	for _, o := range orders {
		// Producer-side guard: validate BEFORE publishing.
		if err := schema.Check(provider, urn, o.data); err != nil {
			if errors.Is(err, schema.ErrInvalidPayload) {
				fmt.Printf("[go] REJECTED   order_id=%v  (%s)\n             %v\n", o.data["order_id"], o.note, err)
				continue // never publishes — invalid data stays out of the queue
			}
			panic(err) // a provider/IO error (e.g. registry unavailable) — not a data problem
		}

		id, err := app.Publish(context.Background(), urn, o.data)
		if err != nil {
			panic(err)
		}
		published++
		fmt.Printf("[go] PUBLISHED  order_id=%v  meta.id=%s  (%s)\n", o.data["order_id"], id, o.note)
	}

	fmt.Printf("[go] validated %d order(s) against %q's schema — published %d, rejected %d before send.\n",
		len(orders), urn, published, len(orders)-published)
}
