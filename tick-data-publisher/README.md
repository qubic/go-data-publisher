# go-tick-data-publisher

Service for publishing qubic tick data messages to a message broker.

## Build

`go build` in the module root directory will create the executable.

## Run tests

`go test -p 1 -tags ci ./...` will run all unit tests.

## Prerequisites

The application needs kafka to be installed. The topics need to be created before starting the application.

## Configuration

## Configuration options

You can use command line properties or environment variables. Environment variables need to be prefixed with `QUBIC_TICK_DATA_PUBLISHER_`.

The following properties (with defaults) can be set:

```bash
--client-archiver-grpc-host=localhost:8010
--broker-bootstrap-servers=localhost:9092
--broker-produce-topic=qubic-tick-data
--sync-server-port=8000
--sync-metrics-port=9999
--sync-metrics-namespace=qubic_kafka
--sync-num-workers=16
--sync-start-tick=0
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
--sync-server-port=
`

Port where to run the non-metrics endpoints (like health check).

`
--sync-metrics-port=
`
Port for exposing prometheus metrics. Access default with `curl localhost:9999/metrics` for example.

`
--sync-metrics-namespace=
`
Namespace (prefix) for prometheus metrics.

`
--sync-num-workers=
`

Number of maximum parallel workers. Retrieving and sending tick data can be slow. Therefore, processing can be done in
batches (maximum number of ticks processed in parallel is equal to number of workers).

`
--sync-start-tick=
`

Allows to override the start tick if set to a value `x > 0`. Attention: this override happens on every start.