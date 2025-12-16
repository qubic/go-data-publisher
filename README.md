# go-data-publisher

This repository contains services that publish qubic on chain data to a message broker for further processing.
It contains producers and consumers. This is a mono repo, see corresponding sub folders for more information about the
services.

## Subprojects

- computors-consumer — consumer for computors data: [computors-consumer/README.md](computors-consumer/README.md)
- computors-publisher — publisher for computors data: [computors-publisher/README.md](computors-publisher/README.md)
- status-service — service exposing status and persistence utilities: [status-service/README.md](status-service/README.md)
- tick-data-consumer — consumer for tick data: [tick-data-consumer/README.md](tick-data-consumer/README.md)
- tick-data-publisher — publisher for tick data: [tick-data-publisher/README.md](tick-data-publisher/README.md)
- tick-intervals-consumer — consumer for tick intervals: [tick-intervals-consumer/README.md](tick-intervals-consumer/README.md)
- tick-intervals-publisher — publisher for tick intervals: [tick-intervals-publisher/README.md](tick-intervals-publisher/README.md)
- transactions-consumer — consumer for transactions: [transactions-consumer/README.md](transactions-consumer/README.md)
- transactions-producer — producer for transactions: [transactions-producer/README.md](transactions-producer/README.md)

Each subproject folder contains details about building, running, configuration, and metrics (when applicable).