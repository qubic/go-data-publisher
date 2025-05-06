package sync

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

type TickDataProducer struct {
	kcl KafkaClient
}

func NewTickDataProducer(client KafkaClient) *TickDataProducer {
	return &TickDataProducer{
		kcl: client,
	}
}
