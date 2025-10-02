FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/tick-data-publisher
COPY . /src/tick-data-publisher

RUN go mod tidy
WORKDIR /src/tick-data-publisher
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/tick-data-publisher/tick-data-publisher /app/tick-data-publisher

RUN chmod +x /app/tick-data-publisher

WORKDIR /app

ENTRYPOINT ["./tick-data-publisher"]