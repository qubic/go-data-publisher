FROM golang:1.25 as builder
ENV CGO_ENABLED=0

WORKDIR /src/computors-consumer
COPY . /src/computors-consumer

RUN go mod tidy
WORKDIR /src/computors-consumer
RUN go build

FROM alpine:latest

COPY --from=builder /src/computors-consumer/computors-consumer /app/computors-consumer

RUN chmod +x /app/computors-consumer

WORKDIR /app

ENTRYPOINT ["./computors-consumer"]