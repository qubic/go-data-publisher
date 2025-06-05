package domain

import (
	cmp2 "cmp"
	"context"
	"errors"
	"github.com/google/go-cmp/cmp"
	"github.com/qubic/transactions-producer/entities"
	"github.com/qubic/transactions-producer/infrastructure/store/pebbledb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

var metrics = NewMetrics("test")
var ErrMock = errors.New("mock error")

type MockFetcher struct {
	processedTickIntervalsPerEpoch []entities.ProcessedTickIntervalsPerEpoch
	shouldError                    bool
	emptyTicks                     []uint32
}

func (mf *MockFetcher) GetProcessedTickIntervalsPerEpoch(_ context.Context) ([]entities.ProcessedTickIntervalsPerEpoch, error) {

	if mf.shouldError {
		return nil, ErrMock
	}

	return mf.processedTickIntervalsPerEpoch, nil
}

func (mf *MockFetcher) GetTickTransactions(_ context.Context, tick uint32) ([]entities.Tx, error) {

	if mf.shouldError {
		return nil, ErrMock
	}

	if mf.emptyTicks != nil && slices.Contains(mf.emptyTicks, tick) {
		return []entities.Tx{}, nil
	}

	return []entities.Tx{
		{
			TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			Amount:     100,
			TickNumber: tick,
			InputType:  0,
			InputSize:  0,
			Input:      "",
			Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:  1744610180,
			MoneyFlew:  true,
		},
	}, nil
}

type MockPublisher struct {
	publishedTickTransactions []entities.TickTransactions
	shouldError               bool
	locker                    sync.Mutex
}

func (mp *MockPublisher) PublishTickTransactions(tickTransactions []entities.TickTransactions) error {

	if mp.shouldError {
		return ErrMock
	}
	mp.locker.Lock() // increment might not work with many threads otherwise
	mp.publishedTickTransactions = append(mp.publishedTickTransactions, tickTransactions...)
	mp.locker.Unlock()
	return nil
}

func TestTxProcessor_process(t *testing.T) {

	fetcher := MockFetcher{
		processedTickIntervalsPerEpoch: []entities.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000001,
						LastProcessedTick:    10000101,
					},
				},
			},
			{
				Epoch: 103,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 40000000,
						LastProcessedTick:    40000000,
					},
					{
						InitialProcessedTick: 50000001,
						LastProcessedTick:    50000010,
					},
				},
			},
		},
	}

	fetcher.emptyTicks = append(fetcher.emptyTicks, 10000042)

	publisher := MockPublisher{}

	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	store, err := pebbledb.NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()
	err = store.SetLastProcessedTick(0)
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, store, 10, logger.Sugar(), metrics)

	err = txProcessor.process() // first interval
	require.NoError(t, err)
	require.Equal(t, 100, len(publisher.publishedTickTransactions)) // one empty tick

	err = txProcessor.process() // second interval
	require.NoError(t, err)
	require.Equal(t, 100+1, len(publisher.publishedTickTransactions))

	err = txProcessor.process() // third interval
	require.NoError(t, err)
	require.Equal(t, 100+1+10, len(publisher.publishedTickTransactions))

	err = txProcessor.process() // no new ticks
	require.NoError(t, err)
	require.Equal(t, 100+1+10, len(publisher.publishedTickTransactions))

}

func TestTxProcessor_RunCycle(t *testing.T) {

	expected := []entities.TickTransactions{
		{
			Epoch:      100,
			TickNumber: 10000001,
			Transactions: []entities.Tx{
				{
					TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
					Amount:     100,
					TickNumber: 10000001,
					InputType:  0,
					InputSize:  0,
					Input:      "",
					Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
					Timestamp:  1744610180,
					MoneyFlew:  true,
				},
			},
		},
		{
			Epoch:      100,
			TickNumber: 10000002,
			Transactions: []entities.Tx{
				{
					TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
					Amount:     100,
					TickNumber: 10000002,
					InputType:  0,
					InputSize:  0,
					Input:      "",
					Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
					Timestamp:  1744610180,
					MoneyFlew:  true,
				},
			},
		},
		{
			Epoch:      103,
			TickNumber: 40000001,
			Transactions: []entities.Tx{
				{
					TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
					Amount:     100,
					TickNumber: 40000001,
					InputType:  0,
					InputSize:  0,
					Input:      "",
					Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
					Timestamp:  1744610180,
					MoneyFlew:  true,
				},
			},
		},
		{
			Epoch:      103,
			TickNumber: 40000002,
			Transactions: []entities.Tx{
				{
					TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
					Amount:     100,
					TickNumber: 40000002,
					InputType:  0,
					InputSize:  0,
					Input:      "",
					Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
					Timestamp:  1744610180,
					MoneyFlew:  true,
				},
			},
		},
		{
			Epoch:      103,
			TickNumber: 50000016,
			Transactions: []entities.Tx{
				{
					TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
					Amount:     100,
					TickNumber: 50000016,
					InputType:  0,
					InputSize:  0,
					Input:      "",
					Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
					Timestamp:  1744610180,
					MoneyFlew:  true,
				},
			},
		},
		{
			Epoch:      103,
			TickNumber: 50000017,
			Transactions: []entities.Tx{
				{
					TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
					DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
					Amount:     100,
					TickNumber: 50000017,
					InputType:  0,
					InputSize:  0,
					Input:      "",
					Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
					Timestamp:  1744610180,
					MoneyFlew:  true,
				},
			},
		},
	}

	fetcher := MockFetcher{
		processedTickIntervalsPerEpoch: []entities.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000001,
						LastProcessedTick:    10000002,
					},
				},
			},
			{
				Epoch: 103,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 40000001,
						LastProcessedTick:    40000002,
					},
					{
						InitialProcessedTick: 50000016,
						LastProcessedTick:    50000017,
					},
				},
			},
		},
	}
	publisher := MockPublisher{}

	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	store, err := pebbledb.NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()
	err = store.SetLastProcessedTick(0)
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, store, 10, logger.Sugar(), metrics)

	err = txProcessor.process() // first interval
	require.NoError(t, err)
	err = txProcessor.process() // second interval
	require.NoError(t, err)
	err = txProcessor.process() // third interval
	require.NoError(t, err)

	require.Equal(t, 6, len(publisher.publishedTickTransactions))

	got := publisher.publishedTickTransactions

	// Make sure the results are sorted by tick number, as the data is added asynchronously
	slices.SortFunc(got, func(a, b entities.TickTransactions) int {
		return cmp2.Compare(a.TickNumber, b.TickNumber)
	})

	if diff := cmp.Diff(expected, got); diff != "" {
		t.Fatalf("Unexpected result: %v", diff)
	}

}

func TestTxProcessor_PublishSingleTicks(t *testing.T) {
	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	store, err := pebbledb.NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	fetcher := MockFetcher{
		processedTickIntervalsPerEpoch: []entities.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000001,
						LastProcessedTick:    10000002,
					},
				},
			},
			{
				Epoch: 103,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 40000001,
						LastProcessedTick:    40000002,
					},
					{
						InitialProcessedTick: 5000000,
						LastProcessedTick:    5000100,
					},
				},
			},
		},
	}
	publisher := MockPublisher{}

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, store, 10, logger.Sugar(), metrics)
	err = txProcessor.PublishSingleTicks([]uint32{10000001, 10000002, 5000020})
	require.NoError(t, err)

	got := publisher.publishedTickTransactions
	assert.Len(t, got, 3)
	assert.Equal(t, 10000001, int(got[0].TickNumber))
	assert.Equal(t, 100, int(got[0].Epoch))
	assert.Len(t, got[0].Transactions, 1)

	assert.Equal(t, 10000002, int(got[1].TickNumber))
	assert.Equal(t, 100, int(got[1].Epoch))
	assert.Len(t, got[0].Transactions, 1)

	assert.Equal(t, 5000020, int(got[2].TickNumber))
	assert.Equal(t, 103, int(got[2].Epoch))
	assert.Len(t, got[0].Transactions, 1)
}

func TestTxProcessor_ProcessBatch(t *testing.T) {

	fetcher := MockFetcher{}
	publisher := MockPublisher{}

	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	store, err := pebbledb.NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, store, 10, logger.Sugar(), metrics)

	testData := []struct {
		name                 string
		shouldFetcherError   bool
		shouldPublisherError bool
		errorExpected        bool
		epochTickIntervals   entities.ProcessedTickIntervalsPerEpoch
	}{
		{
			name:                 "TestNoDependencyError",
			shouldFetcherError:   false,
			shouldPublisherError: false,
			errorExpected:        false,
			epochTickIntervals: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000001,
						LastProcessedTick:    10000002,
					},
					{
						InitialProcessedTick: 10000016,
						LastProcessedTick:    10000017,
					},
				},
			},
		},

		{
			name:                 "TestPublisherError",
			shouldFetcherError:   false,
			shouldPublisherError: true,
			errorExpected:        true,
			epochTickIntervals: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000001,
						LastProcessedTick:    10000002,
					},
					{
						InitialProcessedTick: 10000016,
						LastProcessedTick:    10000017,
					},
				},
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			epoch := testRun.epochTickIntervals.Epoch
			startingTick := testRun.epochTickIntervals.Intervals[0].InitialProcessedTick
			lastTick := testRun.epochTickIntervals.Intervals[len(testRun.epochTickIntervals.Intervals)-1].LastProcessedTick
			fetcher.shouldError = testRun.shouldFetcherError
			publisher.shouldError = testRun.shouldPublisherError

			lptExpected := startingTick
			err = store.SetLastProcessedTick(startingTick)
			require.NoError(t, err)

			err = txProcessor.processTickRange(epoch, startingTick, lastTick)

			if testRun.errorExpected {
				require.Error(t, err)

				lptGot, err := store.GetLastProcessedTick()
				require.NoError(t, err)
				if lptExpected != lptGot {
					t.Fatalf("LPT changed when it should have remained the same. Expected: %d, Got: %d", lptExpected, lptGot)
				}
			} else {
				require.NoError(t, err)
				lptGot, err := store.GetLastProcessedTick()
				require.NoError(t, err)
				if lptExpected == lptGot {
					t.Fatalf("LPT didnt change when it should have. Expected: %d, Got: %d", lptExpected, lptGot)
				}

			}

		})
	}
}
