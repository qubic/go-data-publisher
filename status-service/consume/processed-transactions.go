package consume

import (
	"context"
	"github.com/qubic/status-service/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
}

type ProcessedTransactionsConsumer struct {
	kafkaClient       KafkaClient
	processingMetrics *metrics.Metrics
}

func NewProcessedTransactionsConsumer(client KafkaClient, m *metrics.Metrics) *ProcessedTransactionsConsumer {
	return &ProcessedTransactionsConsumer{
		kafkaClient:       client,
		processingMetrics: m,
	}
}

func (consumer *ProcessedTransactionsConsumer) Consume(ctx context.Context) {

}
