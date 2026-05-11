package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/qubic/transactions-producer/entities"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}
type Client struct {
	kcl KafkaClient
}

func NewClient(kafkaClient KafkaClient) *Client {
	return &Client{
		kcl: kafkaClient,
	}
}

func (kc *Client) PublishTickTransactions(tickTransactions entities.TickTransactions) error {

	wg := sync.WaitGroup{}
	errorChannel := make(chan error, len(tickTransactions.Transactions))

	tickNumber := tickTransactions.TickNumber
	for _, transaction := range tickTransactions.Transactions {

		record, err := createTickTransactionRecord(tickNumber, transaction)
		if err != nil {
			log.Printf("Error while creating record: %v", err)
			errorChannel <- err
			break
		}

		wg.Add(1)
		kc.kcl.Produce(nil, record, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				log.Printf("Error while producing record: %v", err)
				errorChannel <- fmt.Errorf("publishing tick [%d] and transaction [%s]: %w", tickNumber, transaction.TxID, err)
				return
			}
			errorChannel <- nil
		})

	}

	wg.Wait()
	close(errorChannel)

	for err := range errorChannel {
		if err != nil {
			return fmt.Errorf("producing record: %w", err)
		}
	}

	return nil
}

func createTickTransactionRecord(tickNumber uint32, tx entities.Tx) (*kgo.Record, error) {

	payload, err := json.Marshal(tx)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction to json: %w", err)
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, tickNumber)

	return &kgo.Record{
		Key:   key,
		Value: payload,
	}, nil

}
