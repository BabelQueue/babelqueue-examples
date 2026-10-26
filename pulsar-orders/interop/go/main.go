// Interop probe (Go) for the post-R1 Pulsar exhaustion scenarios (ADR-0036).
//
// The Go side never consumes the work queue. It produces the messages the Java and Node
// consumers are asked to fail, and it reads the dead-letter topic back so the runner can assert
// what the other SDKs wrote there.
//
// Subcommands:
//
//	produce    one canonical envelope to QUEUE (plain publish, the §5 property projection)
//	poison     one body that is not an envelope to QUEUE; KIND selects it
//	             noidentity  {"hello":"world","order_id":7}   (a JSON object with no job/urn: decision 4)
//	             binary      ff fe 62 71 00 80                (not valid UTF-8: ADR-0035 base64 wrapper)
//	gate       one VALID envelope to QUEUE whose bq-schema-version property is not the supported one
//	           (ADR-0037 decision 1, broker-bindings §5.7): the body would decode, so a consumer that
//	           settles it was shown to have decided on the property alone
//	watch-dlq  subscribe to <QUEUE>.dlq from the earliest message and print every record
//
// Environment contract:
//
//	PULSAR_URL      pulsar://localhost:6650
//	QUEUE           queue (topic) name
//	KIND            poison only: noidentity | binary
//	VERSION         gate only: the bq-schema-version property value (default "2")
//	EXPECT          watch-dlq: records to wait for (default 1)
//	IDLE            watch-dlq: seconds without a record, after EXPECT were seen, before exiting 0 (default 8)
//	TIMEOUT         watch-dlq: seconds to wait for EXPECT records before exiting 3 (default 90)
//	EXPECT_RAW_HEX  watch-dlq: hex of the poison body, compared with the wrapper's raw (default: none)
//	EXPECT_SHA256   watch-dlq: hex SHA-256 of the exact body a raw version-gate record must carry (default: none)
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//	PROBE go READY
//	PROBE go PUBLISHED id=<meta.id> trace=<trace_id> at_ms=<epoch ms>
//	PROBE go POISONED kind=<kind>
//	PROBE go GATED id=<meta.id> trace=<trace_id> version=<property value> sha256=<hex of the body sent>
//	PROBE go DLQ id=... reason=... dl_attempts=... attempts=... bq_attempts=... bq_job=... trace=...
//	             original_queue=... lang=... shape=envelope|splice|wrapper|raw raw_encoding=... raw_ok=... member_hello=...
//	             has_dead_letter=0|1 body_sha256=<hex of the record body> body_ok=0|1|- bq_schema_version=...
//	             (shape=raw: a canonical envelope with NO dead_letter block, the raw version-gate record)
//	PROBE go DLQ_DONE count=<n>
//	PROBE go TIMEOUT count=<n>
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pulsarsdk "github.com/apache/pulsar-client-go/pulsar"
	"github.com/babelqueue/babelqueue-go"
	bqpulsar "github.com/babelqueue/babelqueue-go/pulsar"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func seconds(key string, def int) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(def) * time.Second
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		fmt.Fprintf(os.Stderr, "go probe: bad %s=%q\n", key, v)
		os.Exit(2)
	}
	return time.Duration(n) * time.Second
}

func die(msg string, err error) {
	fmt.Fprintf(os.Stderr, "go probe: %s: %v\n", msg, err)
	os.Exit(2)
}

// tok makes a value safe for a space-separated key=value line.
func tok(s string) string {
	s = strings.Join(strings.Fields(s), "_")
	if s == "" {
		return "-"
	}
	return s
}

func produce(ctx context.Context, tr *bqpulsar.Transport, queue string) {
	e, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1001, "from": "go"}, babelqueue.WithQueue(queue))
	if err != nil {
		die("make", err)
	}
	body, err := e.Encode()
	if err != nil {
		die("encode", err)
	}
	at := time.Now().UnixMilli()
	if err := tr.Publish(ctx, queue, string(body)); err != nil {
		die("publish", err)
	}
	fmt.Printf("PROBE go PUBLISHED id=%s trace=%s at_ms=%d\n", e.Meta.ID, e.TraceID, at)
}

func poisonBody(kind string) []byte {
	switch kind {
	case "noidentity":
		return []byte(`{"hello":"world","order_id":7}`)
	case "binary":
		return []byte{0xff, 0xfe, 'b', 'q', 0x00, 0x80}
	}
	fmt.Fprintf(os.Stderr, "go probe: unknown KIND %q (noidentity | binary)\n", kind)
	os.Exit(2)
	return nil
}

func poison(ctx context.Context, tr *bqpulsar.Transport, queue string) {
	kind := env("KIND", "noidentity")
	if err := tr.Publish(ctx, queue, string(poisonBody(kind))); err != nil {
		die("publish", err)
	}
	fmt.Printf("PROBE go POISONED kind=%s\n", kind)
}

func str(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return "-"
	}
	if f, ok := v.(float64); ok {
		return strconv.FormatInt(int64(f), 10)
	}
	return tok(fmt.Sprint(v))
}

func nested(m map[string]any, key string) map[string]any {
	if v, ok := m[key].(map[string]any); ok {
		return v
	}
	return map[string]any{}
}

func printRecord(msg pulsarsdk.Message, expectRaw []byte, expectSHA string) {
	props := msg.Properties()
	prop := func(k string) string { return tok(props[k]) }
	var rec map[string]any
	if err := json.Unmarshal(msg.Payload(), &rec); err != nil {
		fmt.Printf("PROBE go DLQ shape=unparsable error=%s\n", tok(err.Error()))
		return
	}
	dl := nested(rec, "dead_letter")
	_, hasDL := rec["dead_letter"]
	shape := "splice"
	switch {
	case rec["job"] != nil && !hasDL:
		shape = "raw"
	case rec["job"] != nil:
		shape = "envelope"
	case rec["raw"] != nil:
		shape = "wrapper"
	}
	sum := sha256.Sum256(msg.Payload())
	bodySHA := hex.EncodeToString(sum[:])
	bodyOK := "-"
	if expectSHA != "" {
		bodyOK = "0"
		if bodySHA == expectSHA {
			bodyOK = "1"
		}
	}
	hasDLFlag := "0"
	if hasDL {
		hasDLFlag = "1"
	}
	rawOK := "-"
	if shape == "wrapper" && expectRaw != nil {
		raw, _ := rec["raw"].(string)
		got := []byte(raw)
		if rec["raw_encoding"] == "base64" {
			if dec, err := base64.StdEncoding.DecodeString(raw); err == nil {
				got = dec
			} else {
				got = nil
			}
		}
		rawOK = "0"
		if string(got) == string(expectRaw) {
			rawOK = "1"
		}
	}
	fmt.Printf("PROBE go DLQ id=%s reason=%s dl_attempts=%s attempts=%s bq_attempts=%s bq_job=%s trace=%s original_queue=%s lang=%s shape=%s raw_encoding=%s raw_ok=%s member_hello=%s has_dead_letter=%s body_sha256=%s body_ok=%s bq_schema_version=%s\n",
		str(nested(rec, "meta"), "id"), str(dl, "reason"), str(dl, "attempts"), str(rec, "attempts"),
		prop("bq-attempts"), prop("bq-job"), str(rec, "trace_id"), str(dl, "original_queue"), str(dl, "lang"),
		shape, str(rec, "raw_encoding"), rawOK, str(rec, "hello"), hasDLFlag, bodySHA, bodyOK, prop("bq-schema-version"))
}

func watchDLQ(client pulsarsdk.Client, queue string) {
	expect, err := strconv.Atoi(env("EXPECT", "1"))
	if err != nil || expect < 1 {
		die("EXPECT", fmt.Errorf("want a positive integer, got %q", os.Getenv("EXPECT")))
	}
	idle := seconds("IDLE", 8)
	deadline := time.Now().Add(seconds("TIMEOUT", 90))
	var expectRaw []byte
	if h := os.Getenv("EXPECT_RAW_HEX"); h != "" {
		if expectRaw, err = hex.DecodeString(h); err != nil {
			die("EXPECT_RAW_HEX", err)
		}
	}
	expectSHA := strings.ToLower(os.Getenv("EXPECT_SHA256"))
	c, err := client.Subscribe(pulsarsdk.ConsumerOptions{
		Topic:                       queue + ".dlq",
		SubscriptionName:            env("DLQ_SUBSCRIPTION", "bq-exhaustion-check"),
		Type:                        pulsarsdk.Exclusive,
		SubscriptionInitialPosition: pulsarsdk.SubscriptionPositionEarliest,
	})
	if err != nil {
		die("subscribe", err)
	}
	defer c.Close()
	fmt.Println("PROBE go READY")
	count := 0
	last := time.Now()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := c.Receive(ctx)
		cancel()
		if err != nil {
			if count >= expect && time.Since(last) >= idle {
				fmt.Printf("PROBE go DLQ_DONE count=%d\n", count)
				return
			}
			continue
		}
		count++
		last = time.Now()
		printRecord(msg, expectRaw, expectSHA)
		_ = c.Ack(msg)
	}
	fmt.Printf("PROBE go TIMEOUT count=%d\n", count)
	os.Exit(3)
}

// gate publishes a VALID envelope with the §5 projection, except that bq-schema-version carries VERSION.
// It uses the raw Pulsar producer: the transport derives the property from the body, and the point is a
// property the body disagrees with. A consumer that decoded the body first would find nothing wrong.
func gate(ctx context.Context, client pulsarsdk.Client, queue string) {
	version := env("VERSION", "2")
	e, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1001, "from": "go"}, babelqueue.WithQueue(queue))
	if err != nil {
		die("make", err)
	}
	body, err := e.Encode()
	if err != nil {
		die("encode", err)
	}
	producer, err := client.CreateProducer(pulsarsdk.ProducerOptions{Topic: queue})
	if err != nil {
		die("producer", err)
	}
	defer producer.Close()
	_, err = producer.Send(ctx, &pulsarsdk.ProducerMessage{
		Payload: body,
		Properties: map[string]string{
			"bq-job":            e.Job,
			"bq-trace-id":       e.TraceID,
			"bq-message-id":     e.Meta.ID,
			"bq-schema-version": version,
			"bq-source-lang":    e.Meta.Lang,
			"bq-attempts":       "0",
		},
	})
	if err != nil {
		die("send", err)
	}
	sum := sha256.Sum256(body)
	fmt.Printf("PROBE go GATED id=%s trace=%s version=%s sha256=%s\n", e.Meta.ID, e.TraceID, tok(version), hex.EncodeToString(sum[:]))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: probe [produce|poison|gate|watch-dlq]")
		os.Exit(2)
	}
	client, err := pulsarsdk.NewClient(pulsarsdk.ClientOptions{URL: env("PULSAR_URL", "pulsar://localhost:6650")})
	if err != nil {
		die("client", err)
	}
	defer client.Close()
	queue := env("QUEUE", "orders")
	switch os.Args[1] {
	case "produce", "poison":
		tr, err := bqpulsar.New(bqpulsar.WithPulsarClient(client))
		if err != nil {
			die("transport", err)
		}
		if os.Args[1] == "produce" {
			produce(context.Background(), tr, queue)
		} else {
			poison(context.Background(), tr, queue)
		}
	case "gate":
		gate(context.Background(), client, queue)
	case "watch-dlq":
		watchDLQ(client, queue)
	default:
		fmt.Fprintln(os.Stderr, "usage: probe [produce|poison|gate|watch-dlq]")
		os.Exit(2)
	}
}
