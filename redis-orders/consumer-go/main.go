// Consumer (Go) — reads the canonical BabelQueue envelopes a producer in ANY
// language put on the shared Redis queue, routes them by URN, and shows which
// language produced each one (meta.lang).
//
//	go run .
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
)

func main() {
	url := os.Getenv("BROKER_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}

	transport, err := bqredis.New(url)
	if err != nil {
		panic(err)
	}
	defer transport.Close()

	app := babelqueue.NewApp(transport, babelqueue.WithDefaultQueue("orders"))

	app.Handle("urn:babel:orders:created", func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] order created  id=%v amount=%v %v  trace=%s  (produced by %q)\n",
			env.Data["order_id"], env.Data["amount"], env.Data["currency"], env.TraceID, env.Meta.Lang)
		return nil
	})
	app.Handle("urn:babel:catalog:item.indexed", func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] item indexed   sku=%v title=%q  (produced by %q)\n",
			env.Data["sku"], env.Data["title"], env.Meta.Lang)
		return nil
	})

	// Drain everything currently queued, then stop (Pop blocks one poll-timeout
	// on the empty queue and returns nil). For a long-running worker use app.Consume.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	processed, err := app.Drain(ctx, "orders", 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[go] processed %d message(s) — same envelope, different language.\n", processed)
}
