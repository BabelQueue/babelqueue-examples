// Consumer (Go) — reads the canonical BabelQueue envelopes a producer in ANY language put
// on a shared Azure Service Bus queue, routes them by URN, and shows which language
// produced each one (meta.lang).
//
//	go run .
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqasb "github.com/babelqueue/babelqueue-go/azureservicebus"
)

// The emulator's well-known development connection string (no secret: it only works locally).
const emulatorConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;" +
	"SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"

func main() {
	connStr := getenv("ASB_CONNECTION_STRING", emulatorConnectionString)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	transport, err := bqasb.New(
		bqasb.WithConnectionString(connStr),
		bqasb.WithMaxWaitTime(5*time.Second), // snappy demo drain; raise for a long-running worker
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

	// Drain everything currently queued, then stop (a receive on the empty queue waits one
	// max-wait window and returns nil). For a long-running worker use app.Consume.
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
