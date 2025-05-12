package sync

import (
	"context"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/status-service/archiver"
	"github.com/qubic/status-service/elastic"
	"github.com/qubic/status-service/metrics"
	"github.com/stretchr/testify/assert"
	"testing"
)

type FakeElasticClient struct {
	faultyTickNumber uint32
	emptyTickNumber  uint32
}

func (f *FakeElasticClient) GetTickData(_ context.Context, tickNumber uint32) (*elastic.TickData, error) {
	panic("implement me")
}

func (f *FakeElasticClient) GetTransactionHashes(_ context.Context, tickNumber uint32) ([]string, error) {
	if tickNumber == f.emptyTickNumber {
		return []string{}, nil
	}
	hashes := []string{
		"hash-1",
		"hash-2",
		"hash-3",
		"hash-4",
		"hash-5",
	}
	if tickNumber == f.faultyTickNumber {
		hashes = append(hashes, "hash-only-in-elastic")
	}
	return hashes, nil
}

type FakeArchiveClient struct {
	faultyTickNumber uint32
	emptyTickNumber  uint32
}

func (f *FakeArchiveClient) GetTickData(_ context.Context, tickNumber uint32) (*protobuff.TickData, error) {
	if tickNumber == f.emptyTickNumber {
		return nil, nil
	}
	hashes := []string{
		"hash-1",
		"hash-2",
		"hash-3",
		"hash-4",
		"hash-5",
	}
	if tickNumber == f.faultyTickNumber {
		hashes = append(hashes, "hash-only-in-archiver")
	}
	return &protobuff.TickData{
		TransactionIds: hashes,
	}, nil
}

func (f *FakeArchiveClient) GetStatus(_ context.Context) (*archiver.Status, error) {

	interval1 := &archiver.TickInterval{
		Epoch: 100,
		From:  1,
		To:    1000,
	}

	interval2 := &archiver.TickInterval{
		Epoch: 123,
		From:  10000,
		To:    123456,
	}

	status := &archiver.Status{
		LatestEpoch:   123,
		LatestTick:    12345,
		TickIntervals: []*archiver.TickInterval{interval1, interval2},
	}

	return status, nil
}

type FakeDataStore struct {
	tick        uint32
	skippedTick uint32
}

func (f *FakeDataStore) AddSkippedTick(tick uint32) error {
	f.skippedTick = tick
	return nil
}

func (f *FakeDataStore) SetLastProcessedTick(tick uint32) error {
	f.tick = tick
	return nil
}

func (f *FakeDataStore) GetLastProcessedTick() (tick uint32, err error) {
	return f.tick, nil
}

var m = metrics.NewMetrics("test")

func TestProcessor_Sync(t *testing.T) {
	archiveClient := &FakeArchiveClient{}
	elasticClient := &FakeElasticClient{}
	dataStore := &FakeDataStore{}
	processor := NewTickProcessor(archiveClient, elasticClient, dataStore, m, Config{})

	err := processor.sync()
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(dataStore.tick))
}

func TestProcessor_Sync_GivenEmptyDoNotCrash(t *testing.T) {
	archiveClient := &FakeArchiveClient{
		emptyTickNumber: 666,
	}
	elasticClient := &FakeElasticClient{
		emptyTickNumber: 666,
	}
	dataStore := &FakeDataStore{}
	processor := NewTickProcessor(archiveClient, elasticClient, dataStore, m, Config{
		SyncTransactions: true,
	})

	err := processor.sync()
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(dataStore.tick))
}

func TestProcessor_Sync_GivenSkipErroneousTicks_ThenStoreAndContinue(t *testing.T) {
	archiveClient := &FakeArchiveClient{
		faultyTickNumber: 10101, // has extra hash
	}
	elasticClient := &FakeElasticClient{
		faultyTickNumber: 666, // has extra hash
	}
	dataStore := &FakeDataStore{}
	processor := NewTickProcessor(archiveClient, elasticClient, dataStore, m, Config{
		SyncTransactions: true,
		SkipTicks:        true,
	})

	err := processor.sync()
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(dataStore.tick))
	assert.Equal(t, 666, int(dataStore.skippedTick))

	err = processor.sync()
	assert.NoError(t, err)
	assert.Equal(t, 12345, int(dataStore.tick))
	assert.Equal(t, 10101, int(dataStore.skippedTick))
}

func TestProcessor_Sync_GivenMissingHashInElastic_ThenError(t *testing.T) {
	archiveClient := &FakeArchiveClient{
		faultyTickNumber: 1000, // has extra hash
	}
	elasticClient := &FakeElasticClient{}
	dataStore := &FakeDataStore{}
	processor := NewTickProcessor(archiveClient, elasticClient, dataStore, m, Config{
		SyncTransactions: true,
	})

	err := processor.sync()
	assert.Error(t, err)
	assert.Equal(t, 999, int(dataStore.tick))
}

func TestProcessor_Sync_GivenMissingHashInArchiver_ThenError(t *testing.T) {
	archiveClient := &FakeArchiveClient{}
	elasticClient := &FakeElasticClient{
		faultyTickNumber: 666, // has extra hash
	}
	dataStore := &FakeDataStore{}
	processor := NewTickProcessor(archiveClient, elasticClient, dataStore, m, Config{
		SyncTransactions: true,
	})

	err := processor.sync()
	assert.Error(t, err)
	assert.Equal(t, 665, int(dataStore.tick))
}
