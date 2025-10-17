package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/qubic/tick-intervals-consumer/domain"
	"github.com/qubic/tick-intervals-consumer/elastic"
)

type KafkaClient interface {
	PollMessages(ctx context.Context) ([]*domain.TickInterval, error)
	Commit(ctx context.Context) error
	AllowRebalance()
}

type ElasticClient interface {
	BulkIndex(ctx context.Context, data []*elastic.EsDocument) error
	FindOverlappingInterval(ctx context.Context, epoch, from, to uint32) (*elastic.Interval, error)
}

type Processor struct {
	kafkaClient   KafkaClient
	elasticClient ElasticClient
}

func NewProcessor(kafkaClient KafkaClient, elasticClient ElasticClient) *Processor {
	return &Processor{
		kafkaClient:   kafkaClient,
		elasticClient: elasticClient,
	}
}

func (p *Processor) Consume() error {
	log.Println("Starting consume loop")
	for {
		count, err := p.consumeBatch(context.Background())
		if err != nil {
			// abort on error and fix issue
			log.Println("Error consuming batch.")
			return fmt.Errorf("consuming batch: %w", err)
		} else {
			log.Printf("Processed [%d] intervals.", count)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (p *Processor) consumeBatch(ctx context.Context) (int, error) {
	// get messages
	defer p.kafkaClient.AllowRebalance()
	intervals, err := p.kafkaClient.PollMessages(ctx)
	if err != nil {
		return 0, fmt.Errorf("poll messages: %w", err)
	}

	filteredIntervals, err := p.filterDuplicates(ctx, intervals)
	if err != nil {
		return 0, fmt.Errorf("filtering intervals: %w", err)
	}

	if len(filteredIntervals) > 0 {
		// send to elastic
		err = p.sendToElastic(ctx, filteredIntervals)
		if err != nil {
			return 0, err
		}
	}

	// commit
	err = p.kafkaClient.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("committing batch: %w", err)
	}
	return len(intervals), nil
}

func (p *Processor) sendToElastic(ctx context.Context, intervals []*domain.TickInterval) error {
	var documents []*elastic.EsDocument
	for _, interval := range intervals {
		if interval == nil ||
			interval.Epoch == 0 || interval.Epoch >= 65535 ||
			interval.From == 0 || interval.To == 0 || interval.From > interval.To {
			return fmt.Errorf("invalid tick interval: %+v", interval)
		}
		log.Printf("Indexing tick interval for epoch [%d]: [%d] - [%d].", interval.Epoch, interval.From, interval.To)
		document, err := convertToDocument(interval)
		if err != nil {
			return err
		}
		documents = append(documents, document)
	}
	err := p.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return fmt.Errorf("elastic indexing: %w", err)
	}
	return nil
}

func (p *Processor) filterDuplicates(ctx context.Context, intervals []*domain.TickInterval) ([]*domain.TickInterval, error) {

	const key string = "%d-%d"
	var temporaryIntervals = make(map[string]domain.TickInterval)

	var filtered []*domain.TickInterval
	for _, interval := range intervals {
		stored, err := p.elasticClient.FindOverlappingInterval(ctx, interval.Epoch, interval.From, interval.To)
		if err != nil {
			log.Printf("Error checking interval: %v", err)
			return nil, err
		}

		// check if there is already a tick interval (same epoch, same start tick, higher end tick) to be
		// ingested and replace, if the new one is larger or ignore the new one
		intervalKey := fmt.Sprintf(key, interval.Epoch, interval.From)
		previous, found := temporaryIntervals[intervalKey]
		newIntervalIsLarger := !found || interval.To > previous.To

		if newIntervalIsLarger {

			if stored == nil {
				temporaryIntervals[intervalKey] = *interval
			} else {
				if interval.Epoch != stored.Epoch || interval.From != stored.From { // illegal state
					// we assume that epoch and start tick always match (they are used as document id in elastic)
					return nil, fmt.Errorf("new interval %v conflicts with stored data", interval)
				} else if interval.To > stored.To {
					// replace if the end tick is larger than in the current interval
					// this can happen at epoch end if an instance doesn't catch the latest tick(s)
					temporaryIntervals[intervalKey] = *interval
				} else { // else ignore because they are equal or smaller
					log.Printf("Ignoring new interval %v because of stored interval %v.", interval, stored)
				}
			}

		} else { // else ignore smaller interval
			log.Printf("Ignoring interval %v because of other new interval %v.", interval, previous)
		}
	}

	for _, v := range temporaryIntervals {
		filtered = append(filtered, &v)
	}

	return filtered, nil
}

func convertToDocument(interval *domain.TickInterval) (*elastic.EsDocument, error) {
	val, err := json.Marshal(interval)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling tick interval %+v: %w", interval, err)
	}
	document := &elastic.EsDocument{
		Id:      fmt.Sprintf("%d-%d", interval.Epoch, interval.From), // we use `epoch-from` as id as the start ticks should always be correct
		Payload: val,
	}
	return document, nil
}
