package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/qubic/tick-data-publisher/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TickDataProducer struct {
	kcl *kgo.Client
}

func NewTickDataProducer(client *kgo.Client) *TickDataProducer {
	return &TickDataProducer{
		kcl: client,
	}
}

func (p *TickDataProducer) SendMessage(ctx context.Context, tickData *domain.TickData) error {
	record, err := createRecord(tickData)
	if err != nil {
		return err
	}
	// we produce synchronously here because we already parallelize before
	if err = p.kcl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return errors.Wrap(err, "failed to produce record")
	}
	return nil
}

func createRecord(tickData *domain.TickData) (*kgo.Record, error) {
	payload, err := json.Marshal(tickData)
	if err != nil {
		return nil, errors.Wrapf(err, "marshalling to json")
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, tickData.TickNumber)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil
}
