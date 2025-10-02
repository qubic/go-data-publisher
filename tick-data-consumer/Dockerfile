FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/tick-data-consumer
COPY . /src/tick-data-consumer

RUN go mod tidy
WORKDIR /src/tick-data-consumer
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/tick-data-consumer/tick-data-consumer /app/tick-data-consumer

RUN chmod +x /app/tick-data-consumer

WORKDIR /app

ENTRYPOINT ["./tick-data-consumer"]