package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/qubic/transactions-producer/entities"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

type MockKafkaClient struct {
	shouldError  bool
	MessageCount uint
}

func (mkc *MockKafkaClient) Produce(_ context.Context, _ *kgo.Record, promise func(*kgo.Record, error)) {

	mkc.MessageCount++

	if mkc.shouldError {
		go promise(nil, errors.New("dummy error"))
		return
	}

	go promise(nil, nil)
}

func TestClient_PublishTransactions(t *testing.T) {

	testTx := []entities.Tx{
		{
			TxID:       "nagnkafzthqkxvbdewcrypgvkwdzkbbyupekzxpyrtdvvmqgugxbdhvmhvef",
			SourceID:   "BTDXTBFYNBMVCGYBRRTNBZAFUBZNTWSRNSLGMTKGTBNJZTPXJLFHNSLVVQGY",
			DestID:     "RLRNPMAFKPPLUZQXJLTTNFSCJMQEHWMWDVJHMAMZGAMEQWSDUFRJKHLCLDTD",
			Amount:     100,
			TickNumber: 50000017,
			Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:  1744610180,
			MoneyFlew:  true,
		},
		{
			TxID:       "nwybffwfxkkvuuaxmqyhnqnpxpkywjuxrhhrnacfahfdfbvrthvqayzimhmr",
			SourceID:   "DXQWUEYPBRQPCWLSERRGSSNKNVXHZRCQJBFSWBYRVQHGCWGNXFJZBYEESUCY",
			DestID:     "FVFNJFSPEVHAZQUSDREUKDEGHNKCZJAYYRLMKVFYCDYYVKYGEUFKRSTTWUFB\n",
			Amount:     1,
			TickNumber: 50000017,
			InputType:  2,
			InputSize:  3,
			Input:      "blah",
			Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:  1744610180,
			MoneyFlew:  false,
		},
		{
			TxID:       "rejzbpzxaqcdahzjwcxbwpzmwtecikjyfiefzgftxmmajpbqadtggmftfagi",
			SourceID:   "SZQFEDERBJSCVQNNACQGQLHNKYCKGMTJAXTLXEERJBJZTBTYYYXNSGPEDRMC",
			DestID:     "LUNPMWFMRZCWDFRUQUMXNXBAKQDBKBQQEDSVRUBGSFUDXBHDURGNJZCSQXFF",
			TickNumber: 50000017,
			Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:  1744610180,
			MoneyFlew:  true,
		},
	}

	testData := []struct {
		name             string
		tickTransactions entities.TickTransactions
		expectedCount    uint
		shouldError      bool
	}{
		{
			name: "TestPublishTransactions_1",
			tickTransactions: entities.TickTransactions{
				Epoch:        100,
				TickNumber:   50000017,
				Transactions: testTx,
			},
			expectedCount: 3,
			shouldError:   false,
		},
		{
			name: "TestPublishTransactions_2",
			tickTransactions: entities.TickTransactions{
				Epoch:        101,
				TickNumber:   10000001,
				Transactions: testTx,
			},
			expectedCount: 3,
			shouldError:   true,
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			mockClient := &MockKafkaClient{
				shouldError: testRun.shouldError,
			}
			kc := NewClient(mockClient)

			err := kc.PublishTickTransactions(testRun.tickTransactions)

			if testRun.shouldError {
				assert.Error(t, err)
				t.Logf("Err: %v", err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, testRun.expectedCount, mockClient.MessageCount)

		})
	}
}
