package archiver

import (
	"github.com/google/go-cmp/cmp"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/transactions-producer/entities"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestArchiverClient_ArchiveTxToEntityTx(t *testing.T) {

	testData := []struct {
		name                 string
		archiverTransactions []*protobuff.TransactionData
		expected             []entities.Tx
	}{
		{
			name: "TestArchiverToEntityFormat_1",
			archiverTransactions: []*protobuff.TransactionData{
				{
					Transaction: &protobuff.Transaction{
						SourceId:     "FZTXBUWQTOWAHBODSZKVMUQRRPDDASKDOQLSDGLIUCVWDSYWIBAKAXRBKEJJ",
						DestId:       "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
						Amount:       0,
						TickNumber:   23582758,
						InputType:    1,
						InputSize:    848,
						InputHex:     "742028e63d80e34835a78c1fc8b6316ae36712068ce007fe286259f67e0b8d1fd821ad5dd9d6b20581a268e6318960867dfc8b6337ee306159969dfe7dd9e895818b59c6aa2d8a2087d19c6499b6b1a78ca338c6276222b7e9f77e9f48a5ea7ba2c7d230895f57d59c69dd56a5f58018666e29689a38aa2d8adf88a583651f8689a1625938c2297c62688e2f801f188df388a2771a25699d36be1c5f62b659a188e1e7e6238799f5fd9568e286918e685a667a2d8861e881f57b620871f17d622615947c61e6b1af60996891976862160dec871f7875a6891ae869eb71228889ed89a1f7c1f1889a95f99b6adf1729fc661ef5f992869e98921c6aa218999a7b5818861c6461e879f1701a266d8a7d61a65a216c1a6629e6875cc655fc871a767e186159765e2785e1f86df17adef88df37a9fa7d9eb88def7ca218a5e687d9c611a17ba20682217c1c865e1e8662087a1d859ea616147ce237e619735f188226871ee88dec6720e86da47b19f88a1a64996876237aa1c6a9a75ed977bdc385dcf885fd6a21767a1c61d98689a25f997871f488a1769a22795e76a17b665e5675aa60624645996a17b876157b59d8560787dbe67e1d7b997609906a5a5871ce7ce176d17a669a8705e07b9f2675867519364da3601947a9ef691a85fd8c69da05e98d68e1966d78651c874dfd6d21a719a8699cf7d57f86a1c7119a6a1d48619d5eda08461f7f5a267e186061d625ec735a171d9a5ee197a1e6649a2665888718d83da88859a78a105e61a8558d7cda56a21f7ae1c5fdf0866198621c855ed8521482e1278215855ec88e17799e986995675a083210852158620f855df6fe137a97c835ec8798987608685d0671b20021680d906719b5e19a6e99d84dc57759f7b2117fde78558871d9078a0f739a166d826559b6617e62d9a67586631e77e1df689975fd897159f705c969d87619de685a97b1796f1d4855f67b5c5645987419e855886819b7961d6517f76a128519768dfb78da070dc85bd987f1a16f59760d94861a083ddb645cb819f05ed8e82a0e84e0f84211851a46697a8320c8321370dd9751f771a0d652096560083e0d845e182a0076218685ec81de184e118220a78e035e21a809e778de06fdc8829f974a1670a157361482d8877ddd795e085e158661b85de8836088761479606000000",
						SignatureHex: "234a4503b71b3e81092c9bacc2bc2436de49097ee94f947f6ff46c583fd62cad8d9c0086ce229dd4301b2b9c5a1ed9511daa6f3cae3b2375cbe98aefac920300",
						TxId:         "czxyxioyrhtkbbinsnhoieectcugxmbscizlynmaieilqhmnwojaekdczaki",
					},
					Timestamp: 1744649165000,
					MoneyFlew: false,
				},
			},
			expected: []entities.Tx{
				{
					TxID:       "czxyxioyrhtkbbinsnhoieectcugxmbscizlynmaieilqhmnwojaekdczaki",
					SourceID:   "FZTXBUWQTOWAHBODSZKVMUQRRPDDASKDOQLSDGLIUCVWDSYWIBAKAXRBKEJJ",
					DestID:     "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
					Amount:     0,
					TickNumber: 23582758,
					InputType:  1,
					InputSize:  848,
					Input:      "dCAo5j2A40g1p4wfyLYxauNnEgaM4Af+KGJZ9n4LjR/YIa1d2dayBYGiaOYxiWCGffyLYzfuMGFZlp3+fdnolYGLWcaqLYogh9GcZJm2saeMozjGJ2Iit+n3fp9Ipep7osfSMIlfV9Wcad1WpfWAGGZuKWiaOKotit+IpYNlH4aJoWJZOMIpfGJoji+AHxiN84iidxolaZ02vhxfYrZZoYjh5+Yjh5n1/ZVo4oaRjmhaZnotiGHogfV7Yghx8X1iJhWUfGHmsa9gmWiRl2hiFg3shx94daaJGuhp63EiiIntiaH3wfGImpX5m2rfFyn8Zh71+ZKGnpiSHGqiGJmae1gYhhxkYeh58XAaJm2KfWGmWiFsGmYp5odcxlX8hxp2fhhhWXZeJ4Xh+G3xet74jfN6n6fZ64je98ohil5ofZxhGhe6IGgiF8HIZeHoZiCHodhZ6mFhR84jfmGXNfGIImhx7ojexnIOhtpHsZ+IoaZJlodiN6ocapp17Zd73Dhdz4hf1qIXZ6HGHZhomiX5l4cfSIoXaaInledqF7Zl5WdapgYkZFmWoXuHYVe1nYVgeH2+Z+HXuZdgmQalpYcc584XbRemaahwXge58mdYZ1GTZNo2AZR6nvaRqF/Yxp2gXpjWjhlm14ZRyHTf1tIacZqGmc99V/hqHHEZpqHUhhnV7aCEYff1omfhhgYdYl7HNaFx2aXuGXoeZkmiZliIcY2D2oiFmnihBeYahVjXzaVqIfeuHF/fCGYZhiHIVe2FIUguEnghWFXsiOF3memGmVZ1oIMhCFIVhiD4Vd9v4TepfINeyHmJh2CGhdBnGyACFoDZBnGbXhmm6Z2E3Fd1n3shF/3nhViHHZB4oPc5oWbYJlWbZhfmLZpnWGYx534d9omXX9iXFZ9wXJadh2Gd5oWpexeW8dSFX2e1xWRZh0GehViGgZt5YdZRf3ahKFGXaN+3jaBw3IW9mH8aFvWXYNlIYaCD3bZFy4GfBe2OgqDoTg+EIRhRpGaXqDIMgyE3Ddl1H3caDWUgllYAg+DYReGCoAdiGGheyB3hhOEYIgp44DXiGoCed43gb9yIKfl0oWcKFXNhSC2Id93XleCF4VhmG4XeiDYIh2FHlgYAAAA=",
					Signature:  "I0pFA7cbPoEJLJuswrwkNt5JCX7pT5R/b/RsWD/WLK2NnACGziKd1DAbK5xaHtlRHapvPK47I3XL6YrvrJIDAA==",
					Timestamp:  1744649165000,
					MoneyFlew:  false,
				},
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			got, err := archiveTxsToEntitiesTx(testRun.archiverTransactions, false)
			require.NoError(t, err)

			if diff := cmp.Diff(testRun.expected, got); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}

		})
	}

}

func TestArchiverClient_ArchiveStatusToEntitiesProcessedTickIntervals(t *testing.T) {

	testData := []struct {
		name                           string
		processedTickIntervalsPerEpoch []*protobuff.ProcessedTickIntervalsPerEpoch
		expected                       []entities.ProcessedTickIntervalsPerEpoch
	}{
		{
			name: "TestArchiverToEntityFormat_1",
			processedTickIntervalsPerEpoch: []*protobuff.ProcessedTickIntervalsPerEpoch{
				{
					Epoch: 100,
					Intervals: []*protobuff.ProcessedTickInterval{
						{
							InitialProcessedTick: 10000000,
							LastProcessedTick:    19999999,
						},
					},
				},
				{
					Epoch: 101,
					Intervals: []*protobuff.ProcessedTickInterval{
						{
							InitialProcessedTick: 20000000,
							LastProcessedTick:    29999999,
						},
					},
				},
				{
					Epoch: 102,
					Intervals: []*protobuff.ProcessedTickInterval{
						{
							InitialProcessedTick: 30000000,
							LastProcessedTick:    39999999,
						},
					},
				},
				{
					Epoch: 103,
					Intervals: []*protobuff.ProcessedTickInterval{
						{
							InitialProcessedTick: 40000000,
							LastProcessedTick:    49999999,
						},
						{
							InitialProcessedTick: 50000005,
							LastProcessedTick:    59999995,
						},
					},
				},
			},
			expected: []entities.ProcessedTickIntervalsPerEpoch{
				{
					Epoch: 100,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 10000000,
							LastProcessedTick:    19999999,
						},
					},
				},
				{
					Epoch: 101,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 20000000,
							LastProcessedTick:    29999999,
						},
					},
				},
				{
					Epoch: 102,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 30000000,
							LastProcessedTick:    39999999,
						},
					},
				},
				{
					Epoch: 103,
					Intervals: []entities.ProcessedTickInterval{
						{
							InitialProcessedTick: 40000000,
							LastProcessedTick:    49999999,
						},
						{
							InitialProcessedTick: 50000005,
							LastProcessedTick:    59999995,
						},
					},
				},
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			got := archiveStatusToEntitiesProcessedTickIntervals(testRun.processedTickIntervalsPerEpoch)

			if diff := cmp.Diff(testRun.expected, got); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}

		})
	}

}

func TestArchiverClient_ArchiveEpochIntervalsToEntitiesEpochIntervals(t *testing.T) {

	testData := []struct {
		name                           string
		processedTickIntervalsPerEpoch *protobuff.ProcessedTickIntervalsPerEpoch
		expected                       entities.ProcessedTickIntervalsPerEpoch
	}{
		{
			name: "TestArchiverToEntityFormat_1",
			processedTickIntervalsPerEpoch: &protobuff.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []*protobuff.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000000,
						LastProcessedTick:    19999999,
					},
				},
			},
			expected: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 100,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000000,
						LastProcessedTick:    19999999,
					},
				},
			},
		},
		{
			name: "TestArchiverToEntityFormat_2",
			processedTickIntervalsPerEpoch: &protobuff.ProcessedTickIntervalsPerEpoch{
				Epoch: 103,
				Intervals: []*protobuff.ProcessedTickInterval{
					{
						InitialProcessedTick: 40000000,
						LastProcessedTick:    49999999,
					},
					{
						InitialProcessedTick: 50000005,
						LastProcessedTick:    59999995,
					},
				},
			},
			expected: entities.ProcessedTickIntervalsPerEpoch{
				Epoch: 103,
				Intervals: []entities.ProcessedTickInterval{
					{
						InitialProcessedTick: 40000000,
						LastProcessedTick:    49999999,
					},
					{
						InitialProcessedTick: 50000005,
						LastProcessedTick:    59999995,
					},
				},
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			got := archiveEpochIntervalsToEntitiesEpochIntervals(testRun.processedTickIntervalsPerEpoch)

			if diff := cmp.Diff(testRun.expected, got); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}
		})
	}

}
