package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/pkg/errors"
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
		return errors.Wrap(err, "creating epoch computor list record")
	}

	err = ecp.kcl.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return errors.Wrap(err, "producing epoch computor list record")
	}

	return nil
}

func createRecord(computorList *domain.EpochComputors) (*kgo.Record, error) {
	payload, err := json.Marshal(computorList)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling to json")
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, computorList.Epoch)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil
}
