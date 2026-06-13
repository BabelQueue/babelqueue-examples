// Consumer (Go) — reads the canonical BabelQueue envelopes a producer in ANY
// language put on a shared Amazon SQS queue, routes them by URN, and shows which
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
	bqsqs "github.com/babelqueue/babelqueue-go/sqs"
)

func main() {
	region := getenv("AWS_REGION", "eu-central-1")
	endpoint := getenv("SQS_ENDPOINT", "http://localhost:9324")
	// ElasticMQ accepts any credentials; real SQS uses your configured ones.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "test")
		os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transport, err := bqsqs.New(ctx,
		bqsqs.WithRegion(region),
		bqsqs.WithEndpoint(endpoint),
		bqsqs.WithWaitTimeSeconds(2), // snappy demo drain; raise for real long-polling
	)
	if err != nil {
		panic(err)
	}

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

	// Drain everything currently queued, then stop (a receive on the empty queue
	// blocks one wait-time and returns nil). For a long-running worker use app.Consume.
	processed, err := app.Drain(ctx, "orders", 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[go] processed %d message(s) — same envelope, different language.\n", processed)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
