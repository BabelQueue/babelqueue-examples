// Consumer (Go) — charges a payment exactly once, even when the SAME envelope is
// delivered twice. It wraps the business handler with idempotency.Wrap (ADR-0022):
// the wrapper dedupes on meta.id (the canonical per-message identity), so a message
// whose id was already processed successfully is acked and skipped instead of run
// again — no double charge under at-least-once delivery.
//
//	go run .
//
// The producer (../producer-python/charge.py) sends the identical envelope twice with
// the same meta.id. The first delivery charges; the second is recognised as a replay
// of an already-processed id and skipped.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/idempotency"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
)

func main() {
	url := os.Getenv("BROKER_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	queue := os.Getenv("QUEUE")
	if queue == "" {
		queue = "payments"
	}

	transport, err := bqredis.New(url)
	if err != nil {
		panic(err)
	}
	defer transport.Close()

	app := babelqueue.NewApp(transport, babelqueue.WithDefaultQueue(queue))

	// The seen-set store backing idempotency.Wrap. InMemoryStore is fine for this
	// single-process demo; a production fleet uses a Redis- or database-backed Store
	// (same three-method interface) so dedupe is shared across workers.
	store := idempotency.NewInMemoryStore()

	// The business handler — the side-effect we must run AT MOST ONCE.
	charge := func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] charged       payment_id=%v amount=%v %v  meta.id=%s  (charged once)\n",
			env.Data["payment_id"], env.Data["amount"], env.Data["currency"], env.Meta.ID)
		return nil
	}

	// Register the idempotent handler. idempotency.Wrap skips an already-seen id
	// silently (returns nil so the runtime acks it); the small reporter below logs
	// the skip so the demo can show "skipped duplicate" on the second delivery.
	guarded := idempotency.Wrap(store, charge)
	app.Handle("urn:babel:payments:charge", func(ctx context.Context, env babelqueue.Envelope) error {
		if seen, _ := store.Seen(ctx, env.Meta.ID); seen {
			fmt.Printf("[go] skipped       payment_id=%v  meta.id=%s  (duplicate — not charged again)\n",
				env.Data["payment_id"], env.Meta.ID)
		}
		return guarded(ctx, env)
	})

	// Drain everything currently queued, then stop. For a long-running worker use app.Consume.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	processed, err := app.Drain(ctx, queue, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[go] handled %d delivery(ies) — charged exactly once, no double charge.\n", processed)
}
