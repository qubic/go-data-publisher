FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/tick-intervals-consumer
COPY . /src/tick-intervals-consumer

RUN go mod tidy
WORKDIR /src/tick-intervals-consumer
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/tick-intervals-consumer/tick-intervals-consumer /app/tick-intervals-consumer

RUN chmod +x /app/tick-intervals-consumer

WORKDIR /app

ENTRYPOINT ["./tick-intervals-consumer"]