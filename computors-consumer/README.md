# computors-consumer
Service for consuming qubic epoch computor list from a message broker.

## TODO
- Add tests

## Building

```shell
go build
```

Running the above command in the project root directory will create the executable `computors-consumer`.

## Testing

```shell
got test -p 1 -tags ci ./...
```

## Prerequisites
The service requires a running kafka installation, as well as an elasticsearch instance.
The ports for the prerequisite services, the elastic index, as well as the kafka topic can be configured. Please refer to the section below.

## Configuration

```
Usage: computors-consumer [options...] [arguments...]

OPTIONS
      --broker-bootstrap-servers  <string>,[string...]  (default: localhost:9092)          
      --broker-consume-topic      <string>              (default: qubic-computors)         
      --broker-consumer-group     <string>              (default: qubic-elastic)           
      --elastic-addresses         <string>,[string...]  (default: https://localhost:9200)  
      --elastic-certificate       <string>              (default: http_ca.crt)             
      --elastic-index-name        <string>              (default: qubic-computors-alias)   
      --elastic-max-retries       <int>                 (default: 15)                      
      --elastic-password          <string>                                                 
      --elastic-username          <string>              (default: qubic-ingestion)         
  -h, --help                                                                               display this help message
      --sync-metrics-namespace    <string>              (default: qubic-kafka)             
      --sync-metrics-port         <int>                 (default: 9999)                    

ENVIRONMENT
  QUBIC_COMPUTORS_CONSUMER_BROKER_BOOTSTRAP_SERVERS  <string>,[string...]  (default: localhost:9092)          
  QUBIC_COMPUTORS_CONSUMER_BROKER_CONSUME_TOPIC      <string>              (default: qubic-computors)         
  QUBIC_COMPUTORS_CONSUMER_BROKER_CONSUMER_GROUP     <string>              (default: qubic-elastic)           
  QUBIC_COMPUTORS_CONSUMER_ELASTIC_ADDRESSES         <string>,[string...]  (default: https://localhost:9200)  
  QUBIC_COMPUTORS_CONSUMER_ELASTIC_CERTIFICATE       <string>              (default: http_ca.crt)             
  QUBIC_COMPUTORS_CONSUMER_ELASTIC_INDEX_NAME        <string>              (default: qubic-computors-alias)   
  QUBIC_COMPUTORS_CONSUMER_ELASTIC_MAX_RETRIES       <int>                 (default: 15)                      
  QUBIC_COMPUTORS_CONSUMER_ELASTIC_PASSWORD          <string>                                                 
  QUBIC_COMPUTORS_CONSUMER_ELASTIC_USERNAME          <string>              (default: qubic-ingestion)         
  QUBIC_COMPUTORS_CONSUMER_SYNC_METRICS_NAMESPACE    <string>              (default: qubic-kafka)             
  QUBIC_COMPUTORS_CONSUMER_SYNC_METRICS_PORT         <int>                 (default: 9999)                    


```