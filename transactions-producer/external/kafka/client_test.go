package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/qubic/transactions-producer/entities"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

type MockKafkaClient struct {
	shouldError     bool
	MessageCount    uint
	ProducedRecords []*kgo.Record
}

func (mkc *MockKafkaClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {

	mkc.MessageCount++
	mkc.ProducedRecords = append(mkc.ProducedRecords, r)

	if mkc.shouldError {
		go promise(nil, errors.New("dummy error"))
		return
	}

	go promise(nil, nil)
}

func TestClient_PublishTransactions(t *testing.T) {

	testTx := []entities.Transaction{
		{
			Hash:        "nagnkafzthqkxvbdewcrypgvkwdzkbbyupekzxpyrtdvvmqgugxbdhvmhvef",
			Source:      "BTDXTBFYNBMVCGYBRRTNBZAFUBZNTWSRNSLGMTKGTBNJZTPXJLFHNSLVVQGY",
			Destination: "RLRNPMAFKPPLUZQXJLTTNFSCJMQEHWMWDVJHMAMZGAMEQWSDUFRJKHLCLDTD",
			Amount:      100,
			TickNumber:  50000017,
			Signature:   "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:   1744610180,
			MoneyFlew:   true,
		},
		{
			Hash:        "nwybffwfxkkvuuaxmqyhnqnpxpkywjuxrhhrnacfahfdfbvrthvqayzimhmr",
			Source:      "DXQWUEYPBRQPCWLSERRGSSNKNVXHZRCQJBFSWBYRVQHGCWGNXFJZBYEESUCY",
			Destination: "FVFNJFSPEVHAZQUSDREUKDEGHNKCZJAYYRLMKVFYCDYYVKYGEUFKRSTTWUFB\n",
			Amount:      1,
			TickNumber:  50000017,
			InputType:   2,
			InputSize:   3,
			InputData:   "blah",
			Signature:   "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:   1744610180,
			MoneyFlew:   false,
		},
		{
			Hash:        "rejzbpzxaqcdahzjwcxbwpzmwtecikjyfiefzgftxmmajpbqadtggmftfagi",
			Source:      "SZQFEDERBJSCVQNNACQGQLHNKYCKGMTJAXTLXEERJBJZTBTYYYXNSGPEDRMC",
			Destination: "LUNPMWFMRZCWDFRUQUMXNXBAKQDBKBQQEDSVRUBGSFUDXBHDURGNJZCSQXFF",
			TickNumber:  50000017,
			Signature:   "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
			Timestamp:   1744610180,
			MoneyFlew:   true,
		},
	}

	testData := []struct {
		name             string
		tickTransactions []entities.Transaction
		expectedCount    uint
		shouldError      bool
	}{
		{
			name:             "TestPublishTransactions_1",
			tickTransactions: testTx,
			expectedCount:    3,
			shouldError:      false,
		},
		{
			name:             "TestPublishTransactions_2",
			tickTransactions: testTx,
			expectedCount:    3,
			shouldError:      true,
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

func TestClient_PublishTransactions_JsonPayload(t *testing.T) {
	tx1 := entities.Transaction{
		Hash:        "nagnkafzthqkxvbdewcrypgvkwdzkbbyupekzxpyrtdvvmqgugxbdhvmhvef",
		Source:      "BTDXTBFYNBMVCGYBRRTNBZAFUBZNTWSRNSLGMTKGTBNJZTPXJLFHNSLVVQGY",
		Destination: "RLRNPMAFKPPLUZQXJLTTNFSCJMQEHWMWDVJHMAMZGAMEQWSDUFRJKHLCLDTD",
		Amount:      100,
		TickNumber:  50000017,
		Signature:   "aabbcc",
		Timestamp:   1744610180,
		MoneyFlew:   true,
	}
	tx2 := entities.Transaction{
		Hash:        "nwybffwfxkkvuuaxmqyhnqnpxpkywjuxrhhrnacfahfdfbvrthvqayzimhmr",
		Source:      "DXQWUEYPBRQPCWLSERRGSSNKNVXHZRCQJBFSWBYRVQHGCWGNXFJZBYEESUCY",
		Destination: "FVFNJFSPEVHAZQUSDREUKDEGHNKCZJAYYRLMKVFYCDYYVKYGEUFKRSTTWUFB",
		Amount:      1,
		TickNumber:  50000017,
		InputType:   2,
		InputSize:   3,
		InputData:   "blah",
		Signature:   "ddeeff",
		Timestamp:   1744610180,
		MoneyFlew:   false,
	}

	mockClient := &MockKafkaClient{}
	kc := NewClient(mockClient)

	err := kc.PublishTickTransactions([]entities.Transaction{tx1, tx2})
	assert.NoError(t, err)
	assert.Len(t, mockClient.ProducedRecords, 2)

	expectedJson1 := `{"hash":"nagnkafzthqkxvbdewcrypgvkwdzkbbyupekzxpyrtdvvmqgugxbdhvmhvef",
						"source":"BTDXTBFYNBMVCGYBRRTNBZAFUBZNTWSRNSLGMTKGTBNJZTPXJLFHNSLVVQGY",
						"destination":"RLRNPMAFKPPLUZQXJLTTNFSCJMQEHWMWDVJHMAMZGAMEQWSDUFRJKHLCLDTD",
						"amount":100,"tickNumber":50000017,"inputType":0,"inputSize":0,"inputData":"",
						"signature":"aabbcc","timestamp":1744610180,"moneyFlew":true}`
	expectedJson2 := `{"hash":"nwybffwfxkkvuuaxmqyhnqnpxpkywjuxrhhrnacfahfdfbvrthvqayzimhmr",
						"source":"DXQWUEYPBRQPCWLSERRGSSNKNVXHZRCQJBFSWBYRVQHGCWGNXFJZBYEESUCY",
						"destination":"FVFNJFSPEVHAZQUSDREUKDEGHNKCZJAYYRLMKVFYCDYYVKYGEUFKRSTTWUFB",
						"amount":1,"tickNumber":50000017,"inputType":2,"inputSize":3,"inputData":"blah",
						"signature":"ddeeff","timestamp":1744610180,"moneyFlew":false}`
	assert.JSONEq(t, expectedJson1, string(mockClient.ProducedRecords[0].Value))
	assert.JSONEq(t, expectedJson2, string(mockClient.ProducedRecords[1].Value))
	assert.Equal(t, 50000017, int(binary.LittleEndian.Uint32(mockClient.ProducedRecords[0].Key)))
	assert.Equal(t, 50000017, int(binary.LittleEndian.Uint32(mockClient.ProducedRecords[1].Key)))

}
