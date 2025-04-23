package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qubic/go-data-publisher/entities"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
)

type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	//ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
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

		record, err := createTxRecord(tx)
		if err != nil {
			log.Printf("Error while creating transaction record: %v", err)
			errorChannel <- err
			break
		}

		wg.Add(1)
		kc.kcl.Produce(ctx, record, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				log.Printf("Error while producing transaction record: %v", err)
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
			return errors.New("encountered errors while producing transaction records")
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
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, tx.TickNumber)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil

}
