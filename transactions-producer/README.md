# transactions-producer

The transactions-producer publishes on-chain transactions to the configured message broker for downstream processing.

## Contents

- Application code under `app/`
- Domain and entities under `domain/` and `entities/`
- External integrations (archiver, kafka) under `external/`
- Infrastructure and storage under `infrastructure/`

## Getting Started

1. Build
   ```bash
   go build ./...
   ```
2. Run
   ```bash
   go run ./transactions-producer/app/transactions-producer
   ```

## Configuration

- See `app/transactions-producer/main.go` for the main entrypoint and flags.
