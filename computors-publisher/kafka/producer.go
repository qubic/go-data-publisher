package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/qubic/computors-publisher/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type EpochComputorsProducer struct {
	kcl *kgo.Client
}

func NewEpochComputorsProducer(client *kgo.Client) *EpochComputorsProducer {
	return &EpochComputorsProducer{kcl: client}
}

func (ecp *EpochComputorsProducer) SendMessage(ctx context.Context, computorList *domain.EpochComputors) error {
	record, err := createRecord(computorList)
	if err != nil {
		return fmt.Errorf("creating epoch computor list record: %w", err)
	}

	err = ecp.kcl.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return fmt.Errorf("producing epoch computor list record: %w", err)
	}

	return nil
}

func createRecord(computorList *domain.EpochComputors) (*kgo.Record, error) {
	payload, err := json.Marshal(computorList)
	if err != nil {
		return nil, fmt.Errorf("marshalling to json: %w", err)
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, computorList.Epoch)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil
}
