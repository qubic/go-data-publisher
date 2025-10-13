package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/qubic/tick-intervals-consumer/domain"
	"github.com/qubic/tick-intervals-consumer/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	kcl                *kgo.Client
	consumeMetrics     *metrics.Metrics
	lastProcessedEpoch uint32
	lastProcessedTick  uint32
}

func NewClient(kafkaClient *kgo.Client, metrics *metrics.Metrics) *Client {
	return &Client{
		kcl:            kafkaClient,
		consumeMetrics: metrics,
	}
}

func (c *Client) PollMessages(ctx context.Context) ([]*domain.TickInterval, error) {
	fetches := c.kcl.PollRecords(ctx, 1000) // batch process max x messages in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// only non-retryable errors are returned.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return nil, errors.New("fetching records")
	}

	var messages []*domain.TickInterval
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		interval, err := unmarshalTickInterval(record)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling record [%s]: %w", string(record.Value), err)
		}
		messages = append(messages, interval)
		c.consumeMetrics.IncProcessedMessages()
		if interval.To > c.lastProcessedTick {
			c.lastProcessedTick = interval.To
			c.lastProcessedEpoch = interval.Epoch
		}
	}

	return messages, nil
}

// AllowRebalance needs to be called after polling in case option BlockRebalanceOnPoll is set
func (c *Client) AllowRebalance() {
	c.kcl.AllowRebalance() // because of the kgo.BlockRebalanceOnPoll() option
}

func (c *Client) Commit(ctx context.Context) error {
	err := c.kcl.CommitUncommittedOffsets(ctx)
	if err != nil {
		return fmt.Errorf("committing offsets: %w", err)
	}
	c.consumeMetrics.SetProcessedTick(c.lastProcessedEpoch, c.lastProcessedTick)
	return nil
}

func unmarshalTickInterval(record *kgo.Record) (*domain.TickInterval, error) {
	var interval domain.TickInterval
	err := json.Unmarshal(record.Value, &interval)
	if err == nil && (interval.Epoch == 0 || interval.From == 0 || interval.To == 0) {
		err = fmt.Errorf("tick interval missing information: %+v", interval)
	}
	return &interval, nil
}
