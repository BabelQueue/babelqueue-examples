// Consumer (Go) for the OTel interop: handles the messages another SDK produced inside a CONSUMER
// span "process <queue>" and writes the finished spans to OTEL_SPAN_FILE.
//
//	BROKER=redis|sqs|kafka|azureservicebus OTEL_SPAN_FILE=consumer.jsonl go run .
//
// The producer carried a W3C traceparent on the transport (ADR-0028); the runtime hands it to
// otel.WrapHandler through the handler context, so each span written here must be a true child of
// the producer's "send <queue>" span. WithSystem is the broker name of ADR-0038 section 3
// (messaging.system); on Kafka WithConsumerGroup also writes messaging.consumer.group.name.
//
// Defaults target the compose files of the matching *-orders example; BROKER_URL, KAFKA_BROKERS,
// SQS_ENDPOINT and ASB_CONNECTION_STRING override them.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	babelqueue "github.com/babelqueue/babelqueue-go"
	bqasb "github.com/babelqueue/babelqueue-go/azureservicebus"
	"github.com/babelqueue/babelqueue-go/kafka"
	bqotel "github.com/babelqueue/babelqueue-go/otel"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
	bqsqs "github.com/babelqueue/babelqueue-go/sqs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const urn = "urn:babel:orders:created"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "PROBE go FAILED:", err)
		os.Exit(1)
	}
}

func run() error {
	broker := os.Getenv("BROKER")
	spanFile := os.Getenv("OTEL_SPAN_FILE")
	if spanFile == "" {
		return fmt.Errorf("OTEL_SPAN_FILE is required")
	}
	queue := getenv("QUEUE", "orders")
	expected, _ := strconv.Atoi(getenv("OTEL_MESSAGES", "3"))
	timeout, _ := strconv.Atoi(getenv("CONSUME_TIMEOUT", "60"))

	exporter, err := newFileExporter(spanFile)
	if err != nil {
		return err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("otel-interop-consumer", trace.WithSchemaURL(bqotel.SchemaURL))

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	var (
		transport babelqueue.Transport
		system    string
		opts      []bqotel.Option
		consume   = false // Kafka consumes until stopped; the others drain the queue
	)
	switch broker {
	case "redis":
		system = "redis"
		transport, err = bqredis.New(getenv("BROKER_URL", "redis://localhost:6379/0"))
	case "sqs":
		system = "aws_sqs"
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
			_ = os.Setenv("AWS_ACCESS_KEY_ID", "test")
			_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
		}
		transport, err = bqsqs.New(ctx,
			bqsqs.WithRegion(getenv("AWS_REGION", "eu-central-1")),
			bqsqs.WithEndpoint(getenv("SQS_ENDPOINT", "http://localhost:9324")),
			bqsqs.WithWaitTimeSeconds(2),
		)
	case "azureservicebus":
		system = "servicebus"
		// The emulator's well-known development connection string (no secret: it only works locally).
		transport, err = bqasb.New(
			bqasb.WithConnectionString(getenv("ASB_CONNECTION_STRING",
				"Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"+
					"SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")),
			bqasb.WithMaxWaitTime(5*time.Second),
		)
	case "kafka":
		system = "kafka"
		group := getenv("GROUP", "otel-interop")
		opts = append(opts, bqotel.WithConsumerGroup(group))
		consume = true
		transport, err = kafka.New(kafka.WithBrokers(getenv("KAFKA_BROKERS", "localhost:9092")), kafka.WithGroupID(group))
	default:
		return fmt.Errorf("unknown BROKER %q (redis, sqs, kafka, azureservicebus)", broker)
	}
	if err != nil {
		return err
	}
	opts = append(opts, bqotel.WithSystem(system))

	app := babelqueue.NewApp(transport, babelqueue.WithDefaultQueue(queue))

	var handled atomic.Int64
	allHandled := make(chan struct{})
	var once sync.Once
	app.Handle(urn, bqotel.WrapHandler(tracer, func(_ context.Context, env babelqueue.Envelope) error {
		fmt.Printf("PROBE go HANDLED id=%s order_id=%v\n", env.Meta.ID, env.Data["order_id"])
		if handled.Add(1) >= int64(expected) {
			once.Do(func() { close(allHandled) })
		}
		return nil
	}, opts...))
	go func() {
		// Let the last message be acked / committed before the context ends the consume loop.
		select {
		case <-allHandled:
			time.Sleep(time.Second)
			cancel()
		case <-ctx.Done():
		}
	}()

	fmt.Println("PROBE go READY")
	if consume {
		_ = app.Consume(ctx) // returns when the context is done: all messages handled, or the timeout
	} else if _, err := app.Drain(ctx, queue, 0); err != nil && ctx.Err() == nil {
		return err
	}
	fmt.Printf("PROBE go DONE consumed=%d\n", handled.Load())
	if handled.Load() < int64(expected) {
		return fmt.Errorf("consumed %d of %d message(s)", handled.Load(), expected)
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
