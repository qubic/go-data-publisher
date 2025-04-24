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

type Client struct {
	kcl *kgo.Client
}

func NewClient(kafkaClient *kgo.Client) *Client {
	return &Client{
		kcl: kafkaClient,
	}
}

func (kc *Client) PublishTransactions(ctx context.Context, txs []entities.Tx, epoch uint32) error {
	return kc.publishTransactions(ctx, txs, epoch, kc.kcl.Produce)
}

type produceFunc func(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))

func (kc *Client) publishTransactions(ctx context.Context, txs []entities.Tx, epoch uint32, produceFunc produceFunc) error {
	wg := sync.WaitGroup{}
	errorChannel := make(chan error, len(txs))

	for _, tx := range txs {

		record, err := createTxRecord(tx, epoch)
		if err != nil {
			log.Printf("Error while creating transaction record: %v", err)
			errorChannel <- err
			break
		}

		wg.Add(1)
		produceFunc(ctx, record, func(_ *kgo.Record, err error) {
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

func createTxRecord(tx entities.Tx, epoch uint32) (*kgo.Record, error) {

	payload, err := json.Marshal(tx)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction to json: %w", err)
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, tx.TickNumber)

	epochHeaderValue := make([]byte, 4)
	binary.LittleEndian.PutUint32(epochHeaderValue, epoch)
	epochHeader := kgo.RecordHeader{
		Key:   "epoch",
		Value: epochHeaderValue,
	}

	return &kgo.Record{
		Headers: []kgo.RecordHeader{epochHeader},
		Key:     key,
		Value:   payload,
	}, nil

}
