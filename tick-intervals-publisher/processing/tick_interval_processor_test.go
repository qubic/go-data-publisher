package processing

import (
	"context"
	"testing"

	"github.com/qubic/tick-intervals-publisher/domain"
	"github.com/qubic/tick-intervals-publisher/metrics"
	"github.com/stretchr/testify/require"
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

	err := proc.process()
	require.NoError(t, err)
	require.Len(t, producer.sent, 3)
	require.Equal(t, intervals[1].Epoch, producer.sent[0].Epoch)
	require.Equal(t, intervals[2].Epoch, producer.sent[1].Epoch)
	require.Equal(t, intervals[3].Epoch, producer.sent[2].Epoch)
}
