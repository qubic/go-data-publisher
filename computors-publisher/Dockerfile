FROM golang:1.24 as builder
ENV CGO_ENABLED=0

WORKDIR /src/computors-publisher
COPY . /src/computors-publisher

RUN go mod tidy
WORKDIR /src/computors-publisher
RUN go build

FROM alpine:latest

COPY --from=builder /src/computors-publisher /app/computors-publisher

RUN chmod +x /app/computors-publisher

WORKDIR /app

ENTRYPOINT ["./computors-publisher"]