package sync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/qubic/tick-data-publisher/domain"
	"github.com/qubic/tick-data-publisher/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
)

type FakeDataStore struct {
	tickNumber int
}

func (f *FakeDataStore) SetLastProcessedTick(tick uint32) error {
	f.tickNumber = int(tick)
	return nil
}

func (f *FakeDataStore) GetLastProcessedTick() (tick uint32, err error) {
	return uint32(f.tickNumber), nil
}

func defaultCreateTickData(tick uint32) (*domain.TickData, error) {
	return &domain.TickData{
		Epoch:      42,
		TickNumber: tick,
	}, nil
}

type FakeArchiveClient struct {
	createTickData func(tickNumber uint32) (*domain.TickData, error)
}

func (f *FakeArchiveClient) GetStatus(_ context.Context) (*domain.Status, error) {
	interval1 := &domain.TickInterval{
		Epoch: 100,
		From:  1,
		To:    1000,
	}

	interval2 := &domain.TickInterval{
		Epoch: 123,
		From:  10001,
		To:    123456,
	}

	status := &domain.Status{
		LatestEpoch:   123,
		LatestTick:    12345,
		TickIntervals: []*domain.TickInterval{interval1, interval2},
	}

	return status, nil
}

func (f *FakeArchiveClient) GetTickData(_ context.Context, tickNumber uint32) (*domain.TickData, error) {
	return f.createTickData(tickNumber)
}

type FakeProducer struct {
	mutex sync.Mutex
	sent  []*domain.TickData
}

func (f *FakeProducer) SendMessage(_ context.Context, td *domain.TickData) error {
	f.mutex.Lock() // we need to lock because of parallelism
	defer f.mutex.Unlock()
	f.sent = append(f.sent, td)
	return nil
}

var m = metrics.NewProcessingMetrics("test")

func TestTickDataProcessor_PublishCustomTicks(t *testing.T) {
	dataStore := &FakeDataStore{}
	archiveClient := &FakeArchiveClient{defaultCreateTickData}
	producer := &FakeProducer{}
	processor := NewTickDataProcessor(dataStore, archiveClient, producer, 32, m)

	err := processor.PublishCustomTicks([]uint32{1, 2, 3})
	require.NoError(t, err)
	assert.Len(t, producer.sent, 3)
	assert.Equal(t, 1, int(producer.sent[0].TickNumber))
	assert.Equal(t, 2, int(producer.sent[1].TickNumber))
	assert.Equal(t, 3, int(producer.sent[2].TickNumber))
}

func TestTickDataProcessor_process(t *testing.T) {
	dataStore := &FakeDataStore{}
	archiveClient := &FakeArchiveClient{defaultCreateTickData}
	producer := &FakeProducer{}
	processor := NewTickDataProcessor(dataStore, archiveClient, producer, 32, m)

	err := processor.process()
	assert.NoError(t, err)
	assert.Equal(t, 1000, dataStore.tickNumber)
	assert.Len(t, producer.sent, 1000)

	err = processor.process()
	assert.NoError(t, err)
	assert.Equal(t, 12345, dataStore.tickNumber) // until latest tick
	assert.Len(t, producer.sent, 2345+1000)      // previous and 1001 to 12345
}

func TestTickDataProcessor_process_doNotSendEmptyTicks(t *testing.T) {
	dataStore := &FakeDataStore{}
	archiveClient := &FakeArchiveClient{
		createTickData: func(tickNumber uint32) (*domain.TickData, error) { return nil, nil },
	}
	producer := &FakeProducer{}
	processor := NewTickDataProcessor(dataStore, archiveClient, producer, 5, m)

	err := processor.processTickRange(context.Background(), 100, 10, 100)
	assert.NoError(t, err)
	assert.Equal(t, 100, dataStore.tickNumber)
	assert.Len(t, producer.sent, 0)
}

func TestTickDataProcessor_isEmpty(t *testing.T) {

	assert.True(t, isEmpty(&domain.TickData{}))
	assert.True(t, isEmpty(nil))
	assert.True(t, isEmpty(&domain.TickData{
		TickNumber: 123,
		Epoch:      65535,
	}))
	assert.True(t, isEmpty(&domain.TickData{
		TickNumber: 1,
	}))
	assert.True(t, isEmpty(&domain.TickData{
		Epoch: 1,
	}))

	assert.False(t, isEmpty(&domain.TickData{
		TickNumber: 1,
		Epoch:      666,
	}))

}

type FakeProducerWithError struct {
	err error
}

func (f *FakeProducerWithError) SendMessage(_ context.Context, _ *domain.TickData) error {
	return f.err
}

func TestTickDataProcessor_StartProcessing_NonRetriableKafkaError(t *testing.T) {
	dataStore := &FakeDataStore{}
	archiveClient := &FakeArchiveClient{defaultCreateTickData}

	// non-retriable Kafka error
	nonRetriableErr := kerr.MessageTooLarge
	producer := &FakeProducerWithError{err: nonRetriableErr}
	processor := NewTickDataProcessor(dataStore, archiveClient, producer, 1, m)

	// run with a timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.StartProcessing()
	}()

	// Wait for the error or timeout
	select {
	case err := <-errChan:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-retriable kafka error")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for non-retriable error")
	}
}

func TestTickDataProcessor_StartProcessing_RetriableKafkaError(t *testing.T) {
	dataStore := &FakeDataStore{}
	archiveClient := &FakeArchiveClient{defaultCreateTickData}

	// retriable Kafka error
	retriableErr := kerr.LeaderNotAvailable
	producer := &FakeProducerWithError{err: retriableErr}
	processor := NewTickDataProcessor(dataStore, archiveClient, producer, 1, m)

	// run with a short timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.StartProcessing()
	}()

	// wait briefly - should continue running with retriable errors
	select {
	case err := <-errChan:
		t.Fatalf("StartProcessing should not exit on retriable error, but got: %v", err)
	case <-time.After(2 * time.Second):
		// expected - StartProcessing continues running with retriable errors
	}
}
