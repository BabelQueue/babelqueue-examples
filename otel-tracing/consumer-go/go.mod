module github.com/babelqueue/babelqueue-examples/otel-tracing/consumer-go

go 1.21

require (
	github.com/babelqueue/babelqueue-go v1.5.0
	github.com/babelqueue/babelqueue-go/otel v0.1.0
	github.com/babelqueue/babelqueue-go/redis v1.0.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.24.0
	go.opentelemetry.io/otel/sdk v1.24.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/redis/go-redis/v9 v9.7.3 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	golang.org/x/sys v0.17.0 // indirect
)
