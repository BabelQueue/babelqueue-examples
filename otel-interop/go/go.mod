module github.com/babelqueue/babelqueue-examples/otel-interop/consumer-go

go 1.26.0

// Published pins. The semconv span shape and WithSystem / WithConsumerGroup need the R1-otel
// releases (ADR-0038), which the pins below target; with BQ_SDK_ROOT, scripts/otel-verify.sh builds
// this probe against the in-repo sources instead (`go mod edit -replace` on a copy). No go.sum is committed (the other probes do the same): the script builds
// with GOFLAGS=-mod=mod.
require (
	github.com/babelqueue/babelqueue-go v1.20.0
	github.com/babelqueue/babelqueue-go/azureservicebus v1.5.0
	github.com/babelqueue/babelqueue-go/kafka v1.7.0
	github.com/babelqueue/babelqueue-go/otel v0.5.0
	github.com/babelqueue/babelqueue-go/redis v1.5.0
	github.com/babelqueue/babelqueue-go/sqs v1.13.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/sdk v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
)
