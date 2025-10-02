FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/status-service
COPY . /src/status-service

RUN go mod tidy
WORKDIR /src/status-service
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/status-service/status-service /app/status-service

RUN chmod +x /app/status-service

WORKDIR /app

ENTRYPOINT ["./status-service"]