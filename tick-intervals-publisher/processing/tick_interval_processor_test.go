package processing

import (
	"context"
	"testing"
	"time"

	"github.com/qubic/tick-intervals-publisher/domain"
	"github.com/qubic/tick-intervals-publisher/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
)

var m = metrics.NewProcessingMetrics("test")

type FakeArchiveClient struct {
	currentEpoch uint32
	currentTick  uint32
	intervals    []*domain.TickInterval
}

func (f *FakeArchiveClient) GetStatus(_ context.Context) (*domain.Status, error) {

	status := &domain.Status{
		LatestEpoch:   f.currentEpoch,
		LatestTick:    f.currentTick,
		TickIntervals: f.intervals,
	}

	return status, nil
}

type FakeDataStore struct {
	epoch uint32
}

func (f *FakeDataStore) SetLastProcessedEpoch(epoch uint32) error {
	f.epoch = epoch
	return nil
}

func (f *FakeDataStore) GetLastProcessedEpoch() (uint32, error) {
	return f.epoch, nil
}

type FakeProducer struct {
	sent []*domain.TickInterval
}

func (f *FakeProducer) SendMessage(_ context.Context, interval *domain.TickInterval) error {
	f.sent = append(f.sent, interval)
	return nil
}

func TestTickIntervalProcessor_process(t *testing.T) {
	db := &FakeDataStore{
		epoch: 42,
	}

	intervals := []*domain.TickInterval{
		{Epoch: 1, From: 1, To: 1000}, // old (ignored)
		{Epoch: 100, From: 1000, To: 1999},
		{Epoch: 100, From: 5000, To: 6000},
		{Epoch: 101, From: 6001, To: 6999},
		{Epoch: 123, From: 10001, To: 123456}, // latest (ignored)
	}

	client := &FakeArchiveClient{
		currentEpoch: 123,
		currentTick:  123456,
		intervals:    intervals,
	}
	producer := &FakeProducer{}
	proc := NewTickIntervalProcessor(db, client, producer, m)

	err := proc.processIntervals()
	require.NoError(t, err)
	require.Len(t, producer.sent, 3)
	require.Equal(t, intervals[1].Epoch, producer.sent[0].Epoch)
	require.Equal(t, intervals[2].Epoch, producer.sent[1].Epoch)
	require.Equal(t, intervals[3].Epoch, producer.sent[2].Epoch)
}

type FakeProducerWithError struct {
	err   error
	count int
}

func (f *FakeProducerWithError) SendMessage(_ context.Context, _ *domain.TickInterval) error {
	f.count = f.count + 1
	return f.err
}

func TestTickIntervalProcessor_StartProcessing_NonRetriableKafkaError(t *testing.T) {
	db := &FakeDataStore{epoch: 0}

	intervals := []*domain.TickInterval{
		{Epoch: 100, From: 1000, To: 1999},
		{Epoch: 101, From: 6001, To: 6999},
	}

	client := &FakeArchiveClient{
		currentEpoch: 101,
		currentTick:  6999,
		intervals:    intervals,
	}

	// non-retriable Kafka error
	nonRetriableErr := kerr.MessageTooLarge
	producer := &FakeProducerWithError{err: nonRetriableErr}
	proc := NewTickIntervalProcessor(db, client, producer, m)

	// run with a timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- proc.StartProcessing()
	}()

	// Wait for the error or timeout
	select {
	case err := <-errChan:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-retriable kafka error")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for non-retriable error")
	}
}

func TestTickIntervalProcessor_StartProcessing_RetriableKafkaError(t *testing.T) {
	db := &FakeDataStore{epoch: 0}

	intervals := []*domain.TickInterval{
		{Epoch: 100, From: 1000, To: 1999},
		{Epoch: 101, From: 6001, To: 6999},
	}

	client := &FakeArchiveClient{
		currentEpoch: 101,
		currentTick:  6999,
		intervals:    intervals,
	}

	// retriable Kafka error
	retriableErr := kerr.LeaderNotAvailable
	producer := &FakeProducerWithError{err: retriableErr}
	proc := NewTickIntervalProcessor(db, client, producer, m)

	// run with a short timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- proc.StartProcessing()
	}()

	// wait briefly - should continue running with retriable errors
	select {
	case err := <-errChan:
		t.Fatalf("StartProcessing should not exit on retriable error, but got: %v", err)
	case <-time.After(2 * time.Second):
		assert.Greater(t, producer.count, 0)
		// This is expected - StartProcessing continues running with retriable errors
	}
}
