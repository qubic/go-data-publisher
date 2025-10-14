## tick intervals publisher

Service for publishing tick intervals to a message broker.

## Build

`go build` in the module root directory will create the executable.

## Run tests

`go test -p 1 -tags ci ./...` will run all unit tests.

## Prerequisites

The application needs kafka to be installed. The topics need to be created before starting the application.

## Configuration

You can use command line properties or environment variables. Environment variables need to be prefixed with `QUBIC_TICK_INTERVALS_PUBLISHER_`.

The following properties (with defaults) can be set:

```bash
--client-archiver-grpc-host=localhost:8010
--broker-bootstrap-servers=localhost:9092
--broker-produce-topic=qubic-tick-intervals
--sync-metrics-port=9999
--sync-metrics-namespace=qubic-kafka
--sync-start-epoch=0
```

`
--client-archiver-grpc-host=
`

Archiver (GRPC) url for retrieving the source data.

`
--broker-bootstrap-servers=
`

Kafka bootstrap server urls.

`
--broker-produce-topic=
`

Name of the topic to send the messages to.

`
--sync-metrics-port=
`

Port for exposing prometheus metrics and health check. Access default with `curl localhost:9999/metrics` for example.

`
--sync-metrics-namespace=
`

Namespace (prefix) for prometheus metrics.

`
--sync-start-epoch=
`

Allows to override the start epoch if set to a value `x > 0`. Attention: this override happens on every start.