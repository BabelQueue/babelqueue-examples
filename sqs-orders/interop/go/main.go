// Interop probe (Go) for the ADR-0037 SQS poison scenarios (broker-bindings §3.5).
//
// The Go side never consumes the work queue. It provisions the queues, produces the bodies the Java, Node and
// .NET consumers are asked to settle, reads <QUEUE>-dlq (and the queue's native dead-letter queue) back, and
// reports the work queue's depth, so the runner asserts from a language that is not the consumer what the
// consumer wrote or left behind.
//
// Subcommands:
//
//	setup      create <QUEUE> (with a short visibility timeout and, unless NATIVE=0, a native RedrivePolicy
//	           towards <QUEUE>-native), <QUEUE>-dlq and <QUEUE>-native; print their URLs
//	poison     one body that is not an envelope to QUEUE_URL; KIND selects it
//	             noidentity  {"hello":"world","order_id":7}   (a JSON object with no job/urn)
//	             notjson     this is not json                 (not JSON at all)
//	depth      the work queue's ApproximateNumberOfMessages (visible) and ...NotVisible (in flight)
//	watch-dlq  receive from WATCH_URL (default DLQ_URL), delete and print every record
//
// Environment contract:
//
//	SQS_ENDPOINT        http://localhost:9324 (ElasticMQ)
//	AWS_REGION          eu-central-1
//	QUEUE               queue name (setup)
//	VISIBILITY          setup: the work queue's visibility timeout in seconds (default 2)
//	NATIVE              setup: 1 (default) adds RedrivePolicy maxReceiveCount=NATIVE_MAX_RECEIVE (default 3) -> <QUEUE>-native
//	QUEUE_URL, DLQ_URL, WATCH_URL
//	KIND, EXPECT, IDLE, TIMEOUT, EXPECT_RAW_HEX (as in artemis-orders/interop/go/main.go), EXPECT_SHA256
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//	PROBE go SETUP queue_url=<url> dlq_url=<url> native_url=<url>
//	PROBE go POISONED kind=<kind> sha256=<hex of the body sent>
//	PROBE go DEPTH visible=<n> in_flight=<n>
//	PROBE go READY
//	PROBE go DLQ ... shape=splice|wrapper|raw|envelope|unparsable ... has_dead_letter=0|1 body_sha256=<hex> body_ok=0|1|-
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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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

func client(ctx context.Context) *sqs.Client {
	// ElasticMQ accepts any credentials; real SQS uses the configured ones.
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(env("AWS_REGION", "eu-central-1")),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		die("config", err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(env("SQS_ENDPOINT", "http://localhost:9324"))
	})
}

func createQueue(ctx context.Context, c *sqs.Client, name string, attrs map[string]string) string {
	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		die("create "+name, err)
	}
	return aws.ToString(out.QueueUrl)
}

func setup(ctx context.Context, c *sqs.Client, queue string) {
	visibility := env("VISIBILITY", "2")
	// The SQS DLQ is <queue>-dlq (broker-bindings §3, ADR-0037 decision 7): a standard SQS queue name may only hold
	// alphanumerics, hyphens and underscores, so <queue>.dlq cannot be created (ElasticMQ answers InvalidAttributeName).
	// The Java, Node and .NET consumers take the DLQ as an explicit URL, which this probe hands them.
	dlq := createQueue(ctx, c, queue+"-dlq", nil)
	attrs := map[string]string{"VisibilityTimeout": visibility}
	native := "-"
	if env("NATIVE", "1") != "0" {
		native = createQueue(ctx, c, queue+"-native", nil)
		maxReceive, err := strconv.Atoi(env("NATIVE_MAX_RECEIVE", "3"))
		if err != nil || maxReceive < 1 {
			die("NATIVE_MAX_RECEIVE", fmt.Errorf("want a positive integer, got %q", os.Getenv("NATIVE_MAX_RECEIVE")))
		}
		policy, _ := json.Marshal(map[string]any{
			"deadLetterTargetArn": "arn:aws:sqs:elasticmq:000000000000:" + queue + "-native",
			"maxReceiveCount":     maxReceive,
		})
		attrs["RedrivePolicy"] = string(policy)
	}
	url := createQueue(ctx, c, queue, attrs)
	fmt.Printf("PROBE go SETUP queue_url=%s dlq_url=%s native_url=%s\n", url, dlq, native)
}

func poisonBody(kind string) []byte {
	switch kind {
	case "noidentity":
		return []byte(`{"hello":"world","order_id":7}`)
	case "notjson":
		return []byte("this is not json")
	}
	fmt.Fprintf(os.Stderr, "go probe: unknown KIND %q (noidentity | notjson)\n", kind)
	os.Exit(2)
	return nil
}

func poison(ctx context.Context, c *sqs.Client, queueURL string) {
	kind := env("KIND", "noidentity")
	body := poisonBody(kind)
	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String(string(body))}); err != nil {
		die("send", err)
	}
	fmt.Printf("PROBE go POISONED kind=%s sha256=%s\n", kind, sha(body))
}

func depth(ctx context.Context, c *sqs.Client, queueURL string) {
	out, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		die("attributes", err)
	}
	fmt.Printf("PROBE go DEPTH visible=%s in_flight=%s\n",
		tok(out.Attributes["ApproximateNumberOfMessages"]), tok(out.Attributes["ApproximateNumberOfMessagesNotVisible"]))
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

func printRecord(payload []byte, expectRaw []byte, expectSHA string) {
	var rec map[string]any
	if err := json.Unmarshal(payload, &rec); err != nil {
		bodyOK := "-"
		if expectSHA != "" {
			bodyOK = "0"
			if sha(payload) == expectSHA {
				bodyOK = "1"
			}
		}
		fmt.Printf("PROBE go DLQ shape=unparsable error=%s has_dead_letter=0 body_sha256=%s body_ok=%s\n", tok(err.Error()), sha(payload), bodyOK)
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

func watchDLQ(ctx context.Context, c *sqs.Client, queueURL string) {
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
	fmt.Println("PROBE go READY")
	count := 0
	last := time.Now()
	for time.Now().Before(deadline) {
		out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			die("receive", err)
		}
		if len(out.Messages) == 0 {
			if count >= expect && time.Since(last) >= idle {
				fmt.Printf("PROBE go DLQ_DONE count=%d\n", count)
				return
			}
			continue
		}
		for _, m := range out.Messages {
			count++
			last = time.Now()
			printRecord([]byte(aws.ToString(m.Body)), expectRaw, expectSHA)
			_, _ = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
	}
	fmt.Printf("PROBE go TIMEOUT count=%d\n", count)
	os.Exit(3)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: probe [setup|poison|depth|watch-dlq]")
		os.Exit(2)
	}
	ctx := context.Background()
	c := client(ctx)
	switch os.Args[1] {
	case "setup":
		setup(ctx, c, env("QUEUE", "orders"))
	case "poison":
		poison(ctx, c, env("QUEUE_URL", ""))
	case "depth":
		depth(ctx, c, env("QUEUE_URL", ""))
	case "watch-dlq":
		watchDLQ(ctx, c, env("WATCH_URL", env("DLQ_URL", "")))
	default:
		fmt.Fprintln(os.Stderr, "usage: probe [setup|poison|depth|watch-dlq]")
		os.Exit(2)
	}
}
