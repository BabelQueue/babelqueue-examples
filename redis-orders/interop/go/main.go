// Interop probe (Go) for the R1 Redis scenarios.
//
// Environment contract (shared by every probe language):
//
//	REDIS_URL    redis://localhost:6379/0
//	QUEUE        queue name
//	LAYOUT       zset | legacy
//	RETRY_AFTER  seconds, or "none" (zset only)
//	MODE         consume | hang | slow
//	HOLD         seconds a "slow" handler (or each message in "drain" mode) holds the message
//	TIMEOUT      seconds to wait for a message before exiting 3
//	DELAY        seconds (produce only)
//	COUNT        number of messages to publish (produce only, default 1)
//	DELAY        0 = plain publish (any layout); > 0 = delayed publish (zset only)
//	FROM         label stored in data.from (produce only, default go)
//	IDLE         drain mode: seconds without a message before exiting 0 (default 3)
//
// Subcommands: "consume" (default) and "produce" (COUNT delayed publishes).
// MODE=drain keeps consuming (acking each message) until the queue stays empty for IDLE seconds.
//
// Output lines (parsed by scripts/lib/interop-common.sh):
//
//	PROBE go READY
//	PROBE go PUBLISHED id=<meta.id> at_ms=<epoch ms>
//	PROBE go RECEIVED id=<meta.id> attempts=<n> at_ms=<epoch ms>
//	PROBE go ACKED id=<meta.id>
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/babelqueue/babelqueue-go"
	bqredis "github.com/babelqueue/babelqueue-go/redis"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func seconds(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "go probe: bad %s=%q\n", key, v)
		os.Exit(2)
	}
	return f
}

func dur(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

func newTransport() *bqredis.Transport {
	var opts []bqredis.Option
	if strings.EqualFold(env("LAYOUT", "zset"), "zset") {
		opts = append(opts, bqredis.WithLaravelLayout(""))
	}
	if ra := env("RETRY_AFTER", ""); ra == "none" {
		opts = append(opts, bqredis.WithoutRetryAfter())
	} else if ra != "" {
		opts = append(opts, bqredis.WithRetryAfter(dur(seconds("RETRY_AFTER", 0))))
	}
	tr, err := bqredis.New(env("REDIS_URL", "redis://localhost:6379/0"), opts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "go probe: transport:", err)
		os.Exit(2)
	}
	return tr
}

func produce(ctx context.Context, tr *bqredis.Transport) {
	queue := env("QUEUE", "orders")
	count := int(seconds("COUNT", 1))
	for i := 0; i < count; i++ {
		env0, err := babelqueue.Make("urn:babel:orders:created", map[string]any{"order_id": 1001 + i, "from": env("FROM", "go")}, babelqueue.WithQueue(queue))
		if err != nil {
			fmt.Fprintln(os.Stderr, "go probe: make:", err)
			os.Exit(2)
		}
		body, err := env0.Encode()
		if err != nil {
			fmt.Fprintln(os.Stderr, "go probe: encode:", err)
			os.Exit(2)
		}
		at := time.Now().UnixMilli()
		// DELAY=0 is a plain publish (works on both layouts); a positive delay needs the zset layout.
		if delay := dur(seconds("DELAY", 0)); delay > 0 {
			err = tr.PublishDelayed(ctx, queue, env0, delay)
		} else {
			err = tr.Publish(ctx, queue, string(body))
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "go probe: publish:", err)
			os.Exit(2)
		}
		fmt.Printf("PROBE go PUBLISHED id=%s at_ms=%d\n", env0.Meta.ID, at)
	}
}

func consume(ctx context.Context, tr *bqredis.Transport) {
	queue := env("QUEUE", "orders")
	mode := env("MODE", "consume")
	fmt.Println("PROBE go READY")
	deadline := time.Now().Add(dur(seconds("TIMEOUT", 30)))
	idle := dur(seconds("IDLE", 3))
	lastMsg := time.Now()
	for time.Now().Before(deadline) {
		msg, err := tr.Pop(ctx, queue, time.Second)
		if err != nil {
			fmt.Fprintln(os.Stderr, "go probe: pop:", err)
			os.Exit(2)
		}
		if msg == nil {
			if mode == "drain" && time.Since(lastMsg) >= idle {
				fmt.Println("PROBE go DRAINED")
				return
			}
			continue
		}
		lastMsg = time.Now()
		got := time.Now().UnixMilli()
		e, err := babelqueue.Decode([]byte(msg.Body))
		if err != nil {
			fmt.Fprintln(os.Stderr, "go probe: decode:", err)
			os.Exit(2)
		}
		fmt.Printf("PROBE go RECEIVED id=%s attempts=%d at_ms=%d\n", e.Meta.ID, e.Attempts, got)
		switch mode {
		case "hang":
			// The runner kill -9's us while the reservation is held. A plain sleep (not `select {}`)
			// keeps the Go runtime's "all goroutines are asleep" deadlock detector from panicking.
			time.Sleep(time.Hour)
		case "slow", "drain":
			// "drain" with HOLD > 0 spreads a shared queue across concurrent consumers.
			time.Sleep(dur(seconds("HOLD", 0)))
		}
		if err := tr.Ack(ctx, msg); err != nil {
			fmt.Fprintln(os.Stderr, "go probe: ack:", err)
			os.Exit(2)
		}
		fmt.Printf("PROBE go ACKED id=%s\n", e.Meta.ID)
		if mode == "drain" {
			continue
		}
		return
	}
	fmt.Println("PROBE go TIMEOUT")
	os.Exit(3)
}

func main() {
	ctx := context.Background()
	tr := newTransport()
	defer tr.Close()
	cmd := "consume"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "produce":
		produce(ctx, tr)
	case "consume":
		consume(ctx, tr)
	default:
		fmt.Fprintln(os.Stderr, "usage: probe [produce|consume]")
		os.Exit(2)
	}
}
