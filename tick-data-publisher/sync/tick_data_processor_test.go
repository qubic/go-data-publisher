package sync

import (
	"context"
	"github.com/qubic/tick-data-publisher/domain"
	"github.com/qubic/tick-data-publisher/metrics"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
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

func defaultCreateTickData(_ uint32) (*domain.TickData, error) {
	return &domain.TickData{}, nil
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
	count int
}

func (f *FakeProducer) SendMessage(_ context.Context, _ *domain.TickData) error {
	f.mutex.Lock() // we need to lock because of parallelism
	defer f.mutex.Unlock()
	f.count++
	return nil
}

var m = metrics.NewProcessingMetrics("test")

func TestTickDataProcessor_process(t *testing.T) {
	dataStore := &FakeDataStore{}
	archiveClient := &FakeArchiveClient{defaultCreateTickData}
	producer := &FakeProducer{}
	processor := NewTickDataProcessor(dataStore, archiveClient, producer, 32, m)

	err := processor.process()
	assert.NoError(t, err)
	assert.Equal(t, 1000, dataStore.tickNumber)
	assert.Equal(t, 1000, producer.count)

	err = processor.process()
	assert.NoError(t, err)
	assert.Equal(t, 12345, dataStore.tickNumber) // until latest tick
	assert.Equal(t, 2345+1000, producer.count)   // previous and 1001 to 12345
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
	assert.Equal(t, 0, producer.count)
}
