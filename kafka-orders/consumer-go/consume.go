// Go reads the same Kafka "orders" topic a Java service produced to, and routes each record
// to a handler by URN. It never knows which language wrote the record — it only sees the
// canonical envelope, routing on the bq-job header (§6 binding). Consume is process-then-
// commit: the offset advances only after the handler returns.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	"github.com/babelqueue/babelqueue-go/kafka"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	tr, err := kafka.New(kafka.WithBrokers(brokers), kafka.WithGroupID("orders-workers"))
	if err != nil {
		panic(err)
	}

	app := babelqueue.NewApp(tr, babelqueue.WithDefaultQueue("orders"))

	app.Handle("urn:babel:orders:created", func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] orders:created  order_id=%v  amount=%v  from lang=%s  trace=%s  attempts=%d\n",
			env.Data["order_id"], env.Data["amount"], env.Meta.Lang, env.TraceID, env.Attempts)
		return nil
	})
	app.Handle("urn:babel:orders:shipped", func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("[go] orders:shipped  order_id=%v  carrier=%v  from lang=%s\n",
			env.Data["order_id"], env.Data["carrier"], env.Meta.Lang)
		return nil
	})

	seconds := 20
	if s := os.Getenv("CONSUME_SECONDS"); s != "" {
		if n, perr := time.ParseDuration(s + "s"); perr == nil {
			seconds = int(n.Seconds())
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()

	fmt.Printf("[go] consuming from 'orders' for %ds (group 'orders-workers')...\n", seconds)
	_ = app.Consume(ctx)
	fmt.Println("[go] done.")
}
