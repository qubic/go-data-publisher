# go-transactions-consumer

Service for consuming qubic transaction messages from a message broker.

## Build

`go build` in the module root directory will create the executable.

## Run tests

`go test -p 1 -tags ci ./...` will run all unit tests.

## Prerequisites

The application needs kafka to be installed. The topics need to be created before starting the application.

## Configuration

## Configuration options

You can use command line properties or environment variables. Environment variables need to be prefixed with `QUBIC_TRANSACTIONS_CONSUMER_`.

The following properties (with defaults) can be set:

```bash
--elastic-addresses=[https://localhost:9200]
--elastic-username=qubic-ingestion
--elastic-password=
--elastic-index-name=qubic-transactions-alias
--elastic-certificate=http_ca.crt
--elastic-max-retries=15
--broker-bootstrap-servers=localhost:9092
--broker-metrics-port=9999
--broker-metrics-namespace=qubic-kafka
--broker-consume-topic=qubic-transactions
--broker-consumer-group=qubic-elastic
```

`
--elastic-addresses=
`
Elasticsearch url(s).

`
--elastic-username=
`
Elasticsearch username that is allowed to index documents.

`
--elastic-password=
`
Password of the elasticsearch user.

`
--elastic-index-name
`
The name of the elasticsearch index to write to. Must be an alias.

`
--elastic-certificate=
`
Path to the ssl certificate of the elasticsearch server for the ssl connection.

`
--elastic-max-retries=
`
Number of maximum retries for indexing elasticsearch documents.

`
--broker-bootstrap-servers=
`
Kafka bootstrap server urls.

`
--broker-metrics-port=
`
Port for exposing prometheus metrics. Access default with `curl localhost:9999/metrics` for example.

`
--broker-metrics-namespace=
`
Namespace (prefix) for prometheus metrics.

`
--broker-consume-topic=
`
Topic name consuming event kafka messages from.

`
--broker-consumer-group=
`
Group name used for consuming messages.
