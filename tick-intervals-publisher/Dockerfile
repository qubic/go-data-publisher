FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/tick-intervals-publisher
COPY . /src/tick-intervals-publisher

RUN go mod tidy
WORKDIR /src/tick-intervals-publisher
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/tick-intervals-publisher/tick-intervals-publisher /app/tick-intervals-publisher

RUN chmod +x /app/tick-intervals-publisher

WORKDIR /app

ENTRYPOINT ["./tick-intervals-publisher"]