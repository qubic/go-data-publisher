package kafka

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
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
	defer c.kcl.AllowRebalance()
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
			return nil, errors.Wrapf(err, "unmarshalling record %s", string(record.Value))
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
		return errors.Wrap(err, "committing offsets")
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
