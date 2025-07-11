# computors-publisher
Service for publishing qubic epoch computor lists to a message broker.

## Building

```shell
go build
```

Running the above command in the project root directory will create the executable `computors-publisher`.

## Testing

```shell
got test -p 1 -tags ci ./...
```

## Prerequisites
The service requires a running kafka installation, as well as an [archiver](https://github.com/qubic/go-archiver) instance.
The ports for the prerequisite services, as well as the kafka topic can be configured. Please refer to the section below.

## Configuration

```
Usage: computors-publisher [options...] [arguments...]

OPTIONS
      --broker-bootstrap-servers    <string>,[string...]  (default: localhost:9092)   
      --broker-produce-topic        <string>              (default: qubic-computors)  
      --client-archiver-grpc-host   <string>              (default: localhost:8010)   
  -h, --help                                                                          display this help message
      --sync-internal-store-folder  <string>              (default: store)            
      --sync-metrics-namespace      <string>              (default: qubic-kafka)      
      --sync-metrics-port           <int>                 (default: 9999)             
      --sync-server-port            <int>                 (default: 8000)
      --sync-start-epoch            <uint32>              (default: 0)             

ENVIRONMENT
  QUBIC_COMPUTORS_PUBLISHER_BROKER_BOOTSTRAP_SERVERS    <string>,[string...]  (default: localhost:9092)   
  QUBIC_COMPUTORS_PUBLISHER_BROKER_PRODUCE_TOPIC        <string>              (default: qubic-computors)  
  QUBIC_COMPUTORS_PUBLISHER_CLIENT_ARCHIVER_GRPC_HOST   <string>              (default: localhost:8010)   
  QUBIC_COMPUTORS_PUBLISHER_SYNC_INTERNAL_STORE_FOLDER  <string>              (default: store)            
  QUBIC_COMPUTORS_PUBLISHER_SYNC_METRICS_NAMESPACE      <string>              (default: qubic-kafka)      
  QUBIC_COMPUTORS_PUBLISHER_SYNC_METRICS_PORT           <int>                 (default: 9999)             
  QUBIC_COMPUTORS_PUBLISHER_SYNC_SERVER_PORT            <int>                 (default: 8000)
  QUBIC_COMPUTORS_PUBLISHER_SYNC_START_EPOCH            <uint32>              (default: 0)                


```