package tx

import (
	cmp2 "cmp"
	"context"
	"errors"
	"github.com/google/go-cmp/cmp"

	"github.com/qubic/go-data-publisher/entities"
	"github.com/qubic/go-data-publisher/infrastructure/store/pebbledb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"slices"
	"testing"
	"time"
)

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
		return nil, entities.ErrEmptyTick
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
}

func (mp *MockPublisher) PublishTickTransactions(_ context.Context, tickTransactions []entities.TickTransactions) error {

	if mp.shouldError {
		return ErrMock
	}

	mp.publishedTickTransactions = append(mp.publishedTickTransactions, tickTransactions...)

	return nil
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

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, time.Second, store, 100, logger.Sugar())

	err = txProcessor.runCycle(2)
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

func TestTxProcessor_GetStartingTicksForEpochs(t *testing.T) {

	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	store, err := pebbledb.NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()

	// For testing case where store entity is found, and LPT matches last tick of last epoch interval
	err = store.SetLastProcessedTick(102, 26000000)
	require.NoError(t, err)
	// For testing case where store entity is found, but epoch didn't finish syncing
	err = store.SetLastProcessedTick(103, 26000001)
	require.NoError(t, err)

	txProcessor := NewProcessor(nil, 0, nil, 0, store, 0, nil)

	testData := []struct {
		name           string
		epochIntervals []entities.ProcessedTickIntervalsPerEpoch
		expected       map[uint32]uint32
	}{
		{
			name: "TestStartingTickForEpochs_1",
			epochIntervals: []entities.ProcessedTickIntervalsPerEpoch{
				{
					Epoch: 100,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 10000000,
							LastProcessedTick:    10000001,
						},
					},
				},
				{
					Epoch: 101,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 23154312,
							LastProcessedTick:    23343121,
						},
						{
							InitialProcessedTick: 24000000,
							LastProcessedTick:    25000000,
						},
					},
				},
				{
					Epoch: 102,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 25000001,
							LastProcessedTick:    26000000,
						},
					},
				},
				{
					Epoch: 103,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 26000001,
							LastProcessedTick:    26000020,
						},
					},
				},
			},
			expected: map[uint32]uint32{
				100: 10000000, // Case: Store entity not found
				101: 23154312, // Case: Store entity not found
				102: 0,        // Case: LPT exists and matches last tick of last interval
				103: 26000002, // Case: LPY exists but epoch didn't finish syncing
			},
		},
		{
			name: "TestStartingTickForEpochs_2",
			epochIntervals: []entities.ProcessedTickIntervalsPerEpoch{
				{
					Epoch: 200,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 15678915,
							LastProcessedTick:    16947864,
						},
					},
				},
				{
					Epoch: 201,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 18764568,
							LastProcessedTick:    19748735,
						},
						{
							InitialProcessedTick: 20648978,
							LastProcessedTick:    21549873,
						},
					},
				},
			},
			expected: map[uint32]uint32{
				200: 15678915,
				201: 18764568,
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			got, err := txProcessor.getStartingTicksForEpochs(testRun.epochIntervals)
			require.NoError(t, err)

			if diff := cmp.Diff(testRun.expected, got); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}

		})
	}

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

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, time.Second, store, 100, logger.Sugar())

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
		// Error on fetcher cannot be tested, as gatherTickTransactionsBatch keeps on retrying in case of error.
		/*{
			name:                 "TestFetcherError",
			shouldFetcherError:   true,
			shouldPublisherError: false,
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
		},*/
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
			fetcher.shouldError = testRun.shouldFetcherError
			publisher.shouldError = testRun.shouldPublisherError

			lptExpected := startingTick
			err = store.SetLastProcessedTick(epoch, startingTick)
			require.NoError(t, err)

			_, err := txProcessor.processBatch(startingTick, testRun.epochTickIntervals)

			if testRun.errorExpected {
				require.Error(t, err)

				lptGot, err := store.GetLastProcessedTick(epoch)
				require.NoError(t, err)
				if lptExpected != lptGot {
					t.Fatalf("LPT changed when it should have remained the same. Expected: %d, Got: %d", lptExpected, lptGot)
				}
			} else {
				require.NoError(t, err)
				lptGot, err := store.GetLastProcessedTick(epoch)
				require.NoError(t, err)
				if lptExpected == lptGot {
					t.Fatalf("LPT didnt change when it should have. Expected: %d, Got: %d", lptExpected, lptGot)
				}

			}

		})
	}
}

func TestTxProcessor_GatherTickTransactionsBatch(t *testing.T) {

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

	txProcessor := NewProcessor(&fetcher, time.Second, &publisher, time.Second, store, 100, logger.Sugar())

	testData := []struct {
		name                     string
		epochTickIntervals       entities.ProcessedTickIntervalsPerEpoch
		emptyTicks               []uint32
		expectedTickTransactions []entities.TickTransactions
	}{
		{
			name: "TestLastIntervalTickIsEmpty",
			epochTickIntervals: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000000,
						LastProcessedTick:    10000003,
					},
				},
			},
			emptyTicks: []uint32{10000003},
			expectedTickTransactions: []entities.TickTransactions{
				{
					Epoch:      100,
					TickNumber: 10000000,
					Transactions: []entities.Tx{
						{
							TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
							DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
							Amount:     100,
							TickNumber: 10000000,
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
					Epoch:        100,
					TickNumber:   10000003,
					Transactions: []entities.Tx{},
				},
			},
		},
		{
			name: "TestEmptyTickInMiddleOfInterval",
			epochTickIntervals: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000000,
						LastProcessedTick:    10000003,
					},
				},
			},
			emptyTicks: []uint32{10000001},
			expectedTickTransactions: []entities.TickTransactions{
				{
					Epoch:      100,
					TickNumber: 10000000,
					Transactions: []entities.Tx{
						{
							TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
							DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
							Amount:     100,
							TickNumber: 10000000,
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
					Epoch:        100,
					TickNumber:   10000001,
					Transactions: []entities.Tx{},
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
					Epoch:      100,
					TickNumber: 10000003,
					Transactions: []entities.Tx{
						{
							TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
							DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
							Amount:     100,
							TickNumber: 10000003,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
					},
				},
			},
		},
		{
			name: "TestNoEmptyTicks",
			epochTickIntervals: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000000,
						LastProcessedTick:    10000003,
					},
				},
			},
			emptyTicks: []uint32{},
			expectedTickTransactions: []entities.TickTransactions{
				{
					Epoch:      100,
					TickNumber: 10000000,
					Transactions: []entities.Tx{
						{
							TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
							DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
							Amount:     100,
							TickNumber: 10000000,
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
					Epoch:      100,
					TickNumber: 10000003,
					Transactions: []entities.Tx{
						{
							TxID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							SourceID:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
							DestID:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
							Amount:     100,
							TickNumber: 10000003,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
					},
				},
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			fetcher.emptyTicks = testRun.emptyTicks

			epoch := testRun.epochTickIntervals.Epoch
			startTick := testRun.epochTickIntervals.Intervals[0].InitialProcessedTick

			gotTxs, _, err := txProcessor.gatherTickTransactionsBatch(epoch, startTick, testRun.epochTickIntervals)
			require.NoError(t, err)

			if diff := cmp.Diff(testRun.expectedTickTransactions, gotTxs); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}

		})
	}

}
