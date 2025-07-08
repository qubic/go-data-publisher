package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qubic/computors-consumer/domain"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
)

type Client struct {
	kcl *kgo.Client
}

func NewClient(kafkaClient *kgo.Client) *Client {
	return &Client{
		kcl: kafkaClient,
	}
}

func (c *Client) PollMessages(ctx context.Context) ([]*domain.EpochComputors, error) {
	fetches := c.kcl.PollRecords(ctx, 100)
	if errs := fetches.Errors(); len(errs) > 0 {
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return nil, errors.New("fetching records")
	}

	var messages []*domain.EpochComputors
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		epochComputors, err := unmarshallEpochComputors(record)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling record %s: %w", string(record.Value), err)
		}
		messages = append(messages, epochComputors)
	}
	return messages, nil
}

func (c *Client) AllowRebalance() {
	c.kcl.AllowRebalance()
}

func (c *Client) Commit(ctx context.Context) error {
	err := c.kcl.CommitUncommittedOffsets(ctx)
	if err != nil {
		return fmt.Errorf("committing offsets: %w", err)
	}
	return nil
}

func unmarshallEpochComputors(record *kgo.Record) (*domain.EpochComputors, error) {
	var epochComputors domain.EpochComputors
	err := json.Unmarshal(record.Value, &epochComputors)
	if err != nil {
		return nil, err
	}
	return &epochComputors, nil
}
