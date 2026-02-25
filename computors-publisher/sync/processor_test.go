package sync

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/qubic/computors-publisher/domain"
	"github.com/qubic/computors-publisher/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestProcessor_computeComputorsChecksum(t *testing.T) {

	data := domain.EpochComputors{
		Epoch:      100,
		TickNumber: 0,
		Identities: []string{
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		},
		Signature: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}

	sum, err := computeComputorsChecksum(data)
	assert.NoError(t, err)
	assert.Equal(t, "846e6cd5a26cd76c361d802bcf12d4c4eb02cf66268ebff18af9e921dae0118f", hex.EncodeToString(sum))
}

type FakeArchiveClient struct {
	status *domain.Status
}

func (f *FakeArchiveClient) GetStatus(_ context.Context) (*domain.Status, error) {
	return f.status, nil
}

func (f *FakeArchiveClient) GetEpochComputors(_ context.Context, epoch uint32) (*domain.EpochComputors, error) {
	return &domain.EpochComputors{
		Epoch:      epoch,
		TickNumber: 1000,
		Identities: []string{"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		Signature:  "test-signature",
	}, nil
}

type FakeDataStore struct {
	lastProcessedEpoch uint32
	checksums          map[uint32][]byte
}

func (f *FakeDataStore) SetLastProcessedEpoch(epoch uint32) error {
	f.lastProcessedEpoch = epoch
	return nil
}

func (f *FakeDataStore) GetLastProcessedEpoch() (uint32, error) {
	return f.lastProcessedEpoch, nil
}

func (f *FakeDataStore) SetLastStoredComputorListSum(epoch uint32, sum []byte) error {
	if f.checksums == nil {
		f.checksums = make(map[uint32][]byte)
	}
	f.checksums[epoch] = sum
	return nil
}

func (f *FakeDataStore) GetLastStoredComputorListSum(epoch uint32) ([]byte, error) {
	if f.checksums == nil {
		return nil, nil
	}
	return f.checksums[epoch], nil
}

type FakeProducerWithError struct {
	err error
}

func (f *FakeProducerWithError) SendMessage(_ context.Context, _ *domain.EpochComputors) error {
	return f.err
}

func TestEpochComputorsProcessor_StartProcessing_NonRetriableKafkaError(t *testing.T) {
	status := &domain.Status{
		LastProcessedTick: domain.ProcessedTick{
			Epoch:      100,
			TickNumber: 1000,
		},
		EpochList: []uint32{100},
		TickIntervals: map[uint32][]*domain.TickInterval{
			100: {{FirstTick: 1000, LastTick: 1999}},
		},
	}

	client := &FakeArchiveClient{status: status}
	store := &FakeDataStore{lastProcessedEpoch: 100}

	// non-retriable Kafka error
	nonRetriableErr := kerr.MessageTooLarge
	producer := &FakeProducerWithError{err: nonRetriableErr}
	m := metrics.NewProcessingMetrics("test_nonretriable")
	proc := NewEpochComputorsProcessor(client, store, producer, m)

	// run with a timeout
	errChan := make(chan error, 1)
	go func() {
		errChan <- proc.StartProcessing()
	}()

	// wait for the error or timeout
	select {
	case err := <-errChan:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-retriable kafka error")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for non-retriable error")
	}
}

func TestEpochComputorsProcessor_StartProcessing_RetriableKafkaError(t *testing.T) {
	status := &domain.Status{
		LastProcessedTick: domain.ProcessedTick{
			Epoch:      100,
			TickNumber: 1000,
		},
		EpochList: []uint32{100},
		TickIntervals: map[uint32][]*domain.TickInterval{
			100: {{FirstTick: 1000, LastTick: 1999}},
		},
	}

	client := &FakeArchiveClient{status: status}
	store := &FakeDataStore{lastProcessedEpoch: 100}

	// retriable Kafka error
	retriableErr := kerr.UnknownTopicOrPartition
	producer := &FakeProducerWithError{err: retriableErr}
	m := metrics.NewProcessingMetrics("test_retriable")
	proc := NewEpochComputorsProcessor(client, store, producer, m)

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
		// expected - StartProcessing continues running with retriable errors
	}
}
