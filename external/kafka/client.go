package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/qubic/go-data-publisher/entities"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}
type Client struct {
	kcl KafkaClient
}

func NewClient(kafkaClient KafkaClient) *Client {
	return &Client{
		kcl: kafkaClient,
	}
}

func (kc *Client) PublishTransactions(ctx context.Context, txs []entities.Tx) error {

	wg := sync.WaitGroup{}
	errorChannel := make(chan error, len(txs))

	for _, tx := range txs {
		wg.Add(1)

		record, err := createTxRecord(tx)
		if err != nil {
			return fmt.Errorf("creating kafka record for transaction: %w", err)
		}

		kc.kcl.Produce(ctx, record, func(_ *kgo.Record, err error) {
			defer wg.Done()

			if err != nil {
				errorChannel <- err
				return
			}
			errorChannel <- nil
		})
	}
	wg.Wait()
	close(errorChannel)

	for err := range errorChannel {
		if err != nil {
			return fmt.Errorf("producing transaction record: %w", err)
		}
	}

	return nil
}

// Sync version
/*func (kc *Client) PublishTransactions(ctx context.Context, txs []entities.Tx) error {

	var records []*kgo.Record

	for _, tx := range txs {
		record, err := createTxRecord(tx)
		if err != nil {
			return fmt.Errorf("creating kafka record for transaction: %w", err)
		}
		records = append(records, record)
	}

	results := kc.kcl.ProduceSync(ctx, records...)
	err := results.FirstErr()
	if err != nil {
		return fmt.Errorf("kafka error: %w", err)
	}

	return nil
}*/

func createTxRecord(tx entities.Tx) (*kgo.Record, error) {

	payload, err := json.Marshal(tx)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction to json: %w", err)
	}
	key := []byte(tx.TxID)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil

}
