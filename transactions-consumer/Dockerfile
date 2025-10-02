FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/transactions-consumer
COPY . /src/transactions-consumer

RUN go mod tidy
WORKDIR /src/transactions-consumer
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/transactions-consumer/transactions-consumer /app/transactions-consumer

RUN chmod +x /app/transactions-consumer

WORKDIR /app

ENTRYPOINT ["./transactions-consumer"]