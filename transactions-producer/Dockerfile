FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src
COPY . /src

RUN go build -o "./bin/transactions-producer" "./app/transactions-producer"

# We don't need golang to run binaries, just use alpine.
FROM alpine
COPY --from=builder /src/bin/transactions-producer /app/transactions-producer

WORKDIR /app

ENTRYPOINT ["./transactions-producer"]
