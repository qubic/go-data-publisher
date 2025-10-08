package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
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

func (kc *Client) PublishTickTransactions(tickTransactions []entities.TickTransactions) error {

	wg := sync.WaitGroup{}
	errorChannel := make(chan error, len(tickTransactions))

	for _, tick := range tickTransactions {

		record, err := createTickTransactionsRecord(tick)
		if err != nil {
			log.Printf("Error while creating tick transactions record: %v", err)
			errorChannel <- err
			break
		}

		wg.Add(1)
		kc.kcl.Produce(nil, record, func(_ *kgo.Record, err error) {
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
			return errors.New("encountered errors while producing tick transaction records")
		}
	}

	return nil
}

func createTickTransactionsRecord(tx entities.TickTransactions) (*kgo.Record, error) {

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
