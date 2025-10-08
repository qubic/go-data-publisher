package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/qubic/tick-interval-publisher/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TickIntervalProducer struct {
	kcl *kgo.Client
}

func NewTickIntervalProducer(client *kgo.Client) *TickIntervalProducer {
	return &TickIntervalProducer{
		kcl: client,
	}
}

func (p *TickIntervalProducer) SendMessage(ctx context.Context, interval *domain.TickInterval) error {
	record, err := createRecord(interval)
	if err != nil {
		return err
	}
	// we produce synchronously here because there are not many intervals
	if err = p.kcl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("failed to produce record: %w", err)
	}
	return nil
}

func createRecord(interval *domain.TickInterval) (*kgo.Record, error) {
	payload, err := json.Marshal(interval)
	if err != nil {
		return nil, fmt.Errorf("marshalling to json: %w", err)
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, interval.Epoch)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil
}
