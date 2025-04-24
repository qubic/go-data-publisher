package kafka

import (
	"context"
	"errors"
	"github.com/qubic/go-data-publisher/entities"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

type MockKafkaClient struct {
	shouldError bool
}

func (mkc *MockKafkaClient) Produce(_ context.Context, _ *kgo.Record, promise func(*kgo.Record, error)) {

	if mkc.shouldError {
		go promise(nil, errors.New("dummy error"))
		return
	}

	go promise(nil, nil)
}

func TestClient_PublishTransactions(t *testing.T) {

	testData := []struct {
		name             string
		tickTransactions []entities.TickTransactions
		shouldError      bool
	}{
		{
			name: "TestPublishTransactions_1",
			tickTransactions: []entities.TickTransactions{
				{
					Epoch:      100,
					TickNumber: 50000017,
					Transactions: []entities.Tx{
						{
							TxID:       "nagnkafzthqkxvbdewcrypgvkwdzkbbyupekzxpyrtdvvmqgugxbdhvmhvef",
							SourceID:   "BTDXTBFYNBMVCGYBRRTNBZAFUBZNTWSRNSLGMTKGTBNJZTPXJLFHNSLVVQGY",
							DestID:     "RLRNPMAFKPPLUZQXJLTTNFSCJMQEHWMWDVJHMAMZGAMEQWSDUFRJKHLCLDTD",
							Amount:     100,
							TickNumber: 50000017,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "nwybffwfxkkvuuaxmqyhnqnpxpkywjuxrhhrnacfahfdfbvrthvqayzimhmr",
							SourceID:   "DXQWUEYPBRQPCWLSERRGSSNKNVXHZRCQJBFSWBYRVQHGCWGNXFJZBYEESUCY",
							DestID:     "FVFNJFSPEVHAZQUSDREUKDEGHNKCZJAYYRLMKVFYCDYYVKYGEUFKRSTTWUFB\n",
							Amount:     100,
							TickNumber: 50000017,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "rejzbpzxaqcdahzjwcxbwpzmwtecikjyfiefzgftxmmajpbqadtggmftfagi",
							SourceID:   "SZQFEDERBJSCVQNNACQGQLHNKYCKGMTJAXTLXEERJBJZTBTYYYXNSGPEDRMC",
							DestID:     "LUNPMWFMRZCWDFRUQUMXNXBAKQDBKBQQEDSVRUBGSFUDXBHDURGNJZCSQXFF",
							Amount:     100,
							TickNumber: 50000017,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "tqppbdzzdbrdwbuikituwhkcwfwhirykwzytviikjnwjndbmhttxubnvffwn",
							SourceID:   "DDKFMYQAALYYKTQTTQRCTUZKANKTPMNQJNVVJMKYUGWSBNUWKZTVFFWTVBAC",
							DestID:     "HZGSBKBTJQVVGZFJUGQUENWSCTZUNZABAPLMBXMAWWGASWCZEHWMXZCNVHGW",
							Amount:     100,
							TickNumber: 50000017,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "aftjxvktnzpmitxrhifbvekiatfuhwkjkcvbdrhjkrzrcxcpijquwvmmycej",
							SourceID:   "XYYMPXCCBHMSCTSDCQMMYDFSVGVGBETKSZMBWDMUKFUDVFFGWWZCNXPMFGRU",
							DestID:     "MMEGZFZNZKZJKJTLGDPUVBHFCDPGPEZCLVXSQCKJWHYBFWPBMMBCSXKADVZG",
							Amount:     100,
							TickNumber: 50000017,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "faykcdpeifqcxbpzwjcvqhhqmcenuwmgpjjkdpmpwygbamzpepxcczbggpax",
							SourceID:   "QGVUHJGVSJLQBVBVQWQJEZRDSBBEGLGSXNDSGXDGXDTGRYMNWCCJVVKUEFCQ",
							DestID:     "KSCDNFGVKSVMWXAUXGMUZUNLCNCRVLPENQXBQCEUDNENPUZHPANFFYXKLESA",
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
			},
			shouldError: false,
		},
		{
			name: "TestPublishTransactions_2",
			tickTransactions: []entities.TickTransactions{
				{
					Epoch:      101,
					TickNumber: 10000001,
					Transactions: []entities.Tx{
						{
							TxID:       "nagnkafzthqkxvbdewcrypgvkwdzkbbyupekzxpyrtdvvmqgugxbdhvmhvef",
							SourceID:   "BTDXTBFYNBMVCGYBRRTNBZAFUBZNTWSRNSLGMTKGTBNJZTPXJLFHNSLVVQGY",
							DestID:     "RLRNPMAFKPPLUZQXJLTTNFSCJMQEHWMWDVJHMAMZGAMEQWSDUFRJKHLCLDTD",
							Amount:     100,
							TickNumber: 10000001,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "nwybffwfxkkvuuaxmqyhnqnpxpkywjuxrhhrnacfahfdfbvrthvqayzimhmr",
							SourceID:   "DXQWUEYPBRQPCWLSERRGSSNKNVXHZRCQJBFSWBYRVQHGCWGNXFJZBYEESUCY",
							DestID:     "FVFNJFSPEVHAZQUSDREUKDEGHNKCZJAYYRLMKVFYCDYYVKYGEUFKRSTTWUFB\n",
							Amount:     100,
							TickNumber: 10000001,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "rejzbpzxaqcdahzjwcxbwpzmwtecikjyfiefzgftxmmajpbqadtggmftfagi",
							SourceID:   "SZQFEDERBJSCVQNNACQGQLHNKYCKGMTJAXTLXEERJBJZTBTYYYXNSGPEDRMC",
							DestID:     "LUNPMWFMRZCWDFRUQUMXNXBAKQDBKBQQEDSVRUBGSFUDXBHDURGNJZCSQXFF",
							Amount:     100,
							TickNumber: 10000001,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "tqppbdzzdbrdwbuikituwhkcwfwhirykwzytviikjnwjndbmhttxubnvffwn",
							SourceID:   "DDKFMYQAALYYKTQTTQRCTUZKANKTPMNQJNVVJMKYUGWSBNUWKZTVFFWTVBAC",
							DestID:     "HZGSBKBTJQVVGZFJUGQUENWSCTZUNZABAPLMBXMAWWGASWCZEHWMXZCNVHGW",
							Amount:     100,
							TickNumber: 10000001,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "aftjxvktnzpmitxrhifbvekiatfuhwkjkcvbdrhjkrzrcxcpijquwvmmycej",
							SourceID:   "XYYMPXCCBHMSCTSDCQMMYDFSVGVGBETKSZMBWDMUKFUDVFFGWWZCNXPMFGRU",
							DestID:     "MMEGZFZNZKZJKJTLGDPUVBHFCDPGPEZCLVXSQCKJWHYBFWPBMMBCSXKADVZG",
							Amount:     100,
							TickNumber: 10000001,
							InputType:  0,
							InputSize:  0,
							Input:      "",
							Signature:  "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
							Timestamp:  1744610180,
							MoneyFlew:  true,
						},
						{
							TxID:       "faykcdpeifqcxbpzwjcvqhhqmcenuwmgpjjkdpmpwygbamzpepxcczbggpax",
							SourceID:   "QGVUHJGVSJLQBVBVQWQJEZRDSBBEGLGSXNDSGXDGXDTGRYMNWCCJVVKUEFCQ",
							DestID:     "KSCDNFGVKSVMWXAUXGMUZUNLCNCRVLPENQXBQCEUDNENPUZHPANFFYXKLESA",
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
			},
			shouldError: true,
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			kc := NewClient(&MockKafkaClient{
				shouldError: testRun.shouldError,
			})

			err := kc.PublishTickTransactions(context.Background(), testRun.tickTransactions)

			if testRun.shouldError {
				assert.Error(t, err)
				t.Logf("Err: %v", err)
				return
			}
			assert.NoError(t, err)

		})
	}
}
