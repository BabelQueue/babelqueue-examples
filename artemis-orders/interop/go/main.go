// Interop probe (Go) for the ADR-0037 Artemis scenarios (version gate and poison, broker-bindings §7).
//
// The Go side never consumes the work queue. It produces the messages the Java (JMS), Node and .NET
// consumers are asked to settle, and it reads <QUEUE>.dlq back over AMQP 1.0 so the runner asserts, from
// a language that is not the consumer, what the consumer wrote there.
//
// Subcommands:
//
//	gate       one VALID envelope to QUEUE whose bq_schema_version application property is not the supported
//	           one (§7.7, ADR-0037 decision 1). The body would decode, so a consumer that settles it was
//	           shown to have decided on the property alone.
//	poison     one body that is not an envelope to QUEUE; KIND selects it
//	             noidentity  {"hello":"world","order_id":7}   (a JSON object with no job/urn)
//	             binary      ff fe 62 71 00 80                (not valid UTF-8: a Data section)
//	watch-dlq  receive from <QUEUE>.dlq (or WATCH_QUEUE) and print every record; EXPECT=0 asserts the queue is empty
//
// Environment contract:
//
//	ARTEMIS_URL     amqp://artemis:artemis@localhost:5672
//	QUEUE           queue (address) name; the runner provisions <QUEUE> and <QUEUE>.dlq as anycast queues
//	KIND            poison only: noidentity | binary
//	VERSION         gate only: the bq_schema_version property value (default "2")
//	WATCH_QUEUE     watch-dlq: the queue to read instead of <QUEUE>.dlq (the runner reads the work queue to prove it drained)
//	EXPECT          watch-dlq: records to wait for (default 1; 0 waits only for the IDLE window)
//	IDLE            watch-dlq: seconds without a record, after EXPECT were seen, before exiting 0 (default 8)
//	TIMEOUT         watch-dlq: seconds to wait for EXPECT records before exiting 3 (default 90)
//	EXPECT_RAW_HEX  watch-dlq: hex of the poison body, compared with the wrapper's raw (default: none)
//	EXPECT_SHA256   watch-dlq: hex SHA-256 of the exact body a raw version-gate record must carry (default: none)
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//	PROBE go READY
//	PROBE go GATED id=<meta.id> trace=<trace_id> version=<property value> sha256=<hex of the body sent>
//	PROBE go POISONED kind=<kind> sha256=<hex of the body sent>
//	PROBE go DLQ id=... reason=... dl_attempts=... attempts=... trace=... original_queue=... lang=...
//	             shape=envelope|splice|wrapper|raw raw_encoding=... raw_ok=... member_hello=...
//	             has_dead_letter=0|1 body_sha256=<hex of the record body> body_ok=0|1|-
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

	"github.com/Azure/go-amqp"
	babelqueue "github.com/babelqueue/babelqueue-go"
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

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func dial(ctx context.Context) (*amqp.Conn, *amqp.Session) {
	conn, err := amqp.Dial(ctx, env("ARTEMIS_URL", "amqp://artemis:artemis@localhost:5672"), nil)
	if err != nil {
		die("dial", err)
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		die("session", err)
	}
	return conn, session
}

// send writes msg to the anycast queue named queue and waits for the broker's disposition.
func send(ctx context.Context, session *amqp.Session, queue string, msg *amqp.Message) {
	sender, err := session.NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
	if err != nil {
		die("sender", err)
	}
	defer func() { _ = sender.Close(ctx) }()
	if err := sender.Send(ctx, msg, nil); err != nil {
		die("send", err)
	}
}

// gate publishes a VALID envelope with the §7 projection, except that bq_schema_version carries VERSION.
func gate(ctx context.Context, session *amqp.Session, queue string) {
	version := env("VERSION", "2")
	e, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1001, "from": "go"}, babelqueue.WithQueue(queue))
	if err != nil {
		die("make", err)
	}
	body, err := e.Encode()
	if err != nil {
		die("encode", err)
	}
	created := time.UnixMilli(e.Meta.CreatedAt).UTC()
	msg := &amqp.Message{
		Value: string(body),
		Properties: &amqp.MessageProperties{
			CorrelationID: e.TraceID,
			CreationTime:  &created,
		},
		Annotations: amqp.Annotations{"x-opt-jms-type": e.Job},
		ApplicationProperties: map[string]any{
			"bq_schema_version": version,
			"bq_attempts":       "0",
			"bq_app_id":         "babelqueue",
			"bq_source_lang":    e.Meta.Lang,
		},
	}
	send(ctx, session, queue, msg)
	fmt.Printf("PROBE go GATED id=%s trace=%s version=%s sha256=%s\n", e.Meta.ID, e.TraceID, tok(version), sha(body))
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

// poison publishes a body that is not an envelope. A text body is an AMQP value string (what every BabelQueue
// transport writes); a body that is not valid UTF-8 cannot be a string, so it is a Data section.
func poison(ctx context.Context, session *amqp.Session, queue string) {
	kind := env("KIND", "noidentity")
	body := poisonBody(kind)
	var msg *amqp.Message
	if kind == "binary" {
		msg = amqp.NewMessage(body)
	} else {
		msg = &amqp.Message{Value: string(body)}
	}
	send(ctx, session, queue, msg)
	fmt.Printf("PROBE go POISONED kind=%s sha256=%s\n", kind, sha(body))
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

// bodyBytes returns the exact body bytes of a message: a Data section, or an AMQP value (string or binary).
func bodyBytes(msg *amqp.Message) []byte {
	if len(msg.Data) > 0 {
		var out []byte
		for _, d := range msg.Data {
			out = append(out, d...)
		}
		return out
	}
	switch v := msg.Value.(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	}
	return nil
}

func printRecord(payload []byte, expectRaw []byte, expectSHA string) {
	var rec map[string]any
	if err := json.Unmarshal(payload, &rec); err != nil {
		fmt.Printf("PROBE go DLQ shape=unparsable error=%s body_sha256=%s\n", tok(err.Error()), sha(payload))
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
	bodySHA := sha(payload)
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
	fmt.Printf("PROBE go DLQ id=%s reason=%s dl_attempts=%s attempts=%s trace=%s original_queue=%s lang=%s shape=%s raw_encoding=%s raw_ok=%s member_hello=%s has_dead_letter=%s body_sha256=%s body_ok=%s\n",
		str(nested(rec, "meta"), "id"), str(dl, "reason"), str(dl, "attempts"), str(rec, "attempts"),
		str(rec, "trace_id"), str(dl, "original_queue"), str(dl, "lang"),
		shape, str(rec, "raw_encoding"), rawOK, str(rec, "hello"), hasDLFlag, bodySHA, bodyOK)
}

func watchDLQ(ctx context.Context, session *amqp.Session, queue string) {
	expect, err := strconv.Atoi(env("EXPECT", "1"))
	if err != nil || expect < 0 {
		die("EXPECT", fmt.Errorf("want a non-negative integer, got %q", os.Getenv("EXPECT")))
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
	receiver, err := session.NewReceiver(ctx, env("WATCH_QUEUE", queue+".dlq"), &amqp.ReceiverOptions{
		SourceCapabilities: []string{"queue"},
		Credit:             10,
	})
	if err != nil {
		die("receiver", err)
	}
	defer func() { _ = receiver.Close(ctx) }()
	fmt.Println("PROBE go READY")
	count := 0
	last := time.Now()
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := receiver.Receive(rctx, nil)
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
		printRecord(bodyBytes(msg), expectRaw, expectSHA)
		_ = receiver.AcceptMessage(ctx, msg)
	}
	fmt.Printf("PROBE go TIMEOUT count=%d\n", count)
	os.Exit(3)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: probe [gate|poison|watch-dlq]")
		os.Exit(2)
	}
	ctx := context.Background()
	conn, session := dial(ctx)
	defer func() { _ = conn.Close() }()
	queue := env("QUEUE", "orders")
	switch os.Args[1] {
	case "gate":
		gate(ctx, session, queue)
	case "poison":
		poison(ctx, session, queue)
	case "watch-dlq":
		watchDLQ(ctx, session, queue)
	default:
		fmt.Fprintln(os.Stderr, "usage: probe [gate|poison|watch-dlq]")
		os.Exit(2)
	}
}
