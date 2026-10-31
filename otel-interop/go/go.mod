module github.com/babelqueue/babelqueue-examples/otel-interop/consumer-go

go 1.25.0

// Published pins. The semconv span shape and WithSystem / WithConsumerGroup need the R1-otel
// releases (ADR-0038); until they are published, scripts/otel-verify.sh builds this probe against
// the in-repo sources (BQ_SDK_ROOT: `go mod edit -replace` on a copy), and the pins below are bumped
// to the release versions. No go.sum is committed (the other probes do the same): the script builds
// with GOFLAGS=-mod=mod.
require (
	github.com/babelqueue/babelqueue-go v1.18.0
	github.com/babelqueue/babelqueue-go/azureservicebus v1.3.0
	github.com/babelqueue/babelqueue-go/kafka v1.5.0
	github.com/babelqueue/babelqueue-go/otel v0.3.0
	github.com/babelqueue/babelqueue-go/redis v1.4.0
	github.com/babelqueue/babelqueue-go/sqs v1.12.0
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
)
