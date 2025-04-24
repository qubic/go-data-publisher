package pebbledb

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestPebbleStore_LastProcessedTick(t *testing.T) {

	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	store, err := NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()

	testData := []struct {
		name     string
		epoch    uint32
		expected uint32
	}{
		{
			name:     "TestLastProcessedTick_1",
			epoch:    0,
			expected: 10000000,
		},
		{
			name:     "TestLastProcessedTick_2",
			epoch:    1,
			expected: 20048336,
		},
		{
			name:     "TestLastProcessedTick_3",
			epoch:    2,
			expected: 30216726,
		},
		{
			name:     "TestLastProcessedTick_4",
			epoch:    3,
			expected: 60326734,
		},
		{
			name:     "TestLastProcessedTick_5",
			epoch:    4,
			expected: 10048735,
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {
			err := store.SetLastProcessedTick(testRun.epoch, testRun.expected)
			require.NoError(t, err)

			got, err := store.GetLastProcessedTick(testRun.epoch)
			require.NoError(t, err)
			require.Equal(t, testRun.expected, got)
		})
	}

}

func TestPebbleStore_LastProcessedTickPerEpochs(t *testing.T) {
	dbDir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	store, err := NewProcessorStore(dbDir)
	require.NoError(t, err)
	defer store.Close()

	testData := []struct {
		name     string
		expected map[uint32]uint32
	}{
		{
			name: "TestLastProcessedTickPerEpoch_1",
			expected: map[uint32]uint32{
				0:   12677512,
				10:  35498135,
				28:  36485435,
				38:  95436575,
				45:  16548956,
				105: 96548641,
			},
		},
		{
			name: "TestLastProcessedTickPerEpoch_2",
			expected: map[uint32]uint32{
				0: 0,
				1: 10000000,
				2: 20000000,
				3: 30002000,
				4: 49999999,
				5: 56644823,
			},
		},
		{
			name: "TestLastProcessedTickPerEpoch_3",
			expected: map[uint32]uint32{
				104: 13461047,
				105: 13543974,
				106: 13669355,
				107: 13773992,
				108: 13921573,
			},
		},
		{
			name: "TestLastProcessedTickPerEpoch_4",
			expected: map[uint32]uint32{
				201: 16687512,
				105: 32438135,
				108: 31385635,
				215: 92436476,
				411: 13558959,
				306: 11538541,
			},
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {

			for epoch, tickNumber := range testRun.expected {
				err := store.SetLastProcessedTick(epoch, tickNumber)
				require.NoError(t, err)
			}

			got, err := store.GetLastProcessedTickForAllEpochs()
			require.NoError(t, err)

			if diff := cmp.Diff(testRun.expected, got); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}

			for epoch := range testRun.expected {

				key := []byte{lastProcessedTickPerEpochKey}
				key = binary.BigEndian.AppendUint32(key, epoch)

				err := store.db.Delete(key, pebble.Sync)
				require.NoError(t, err)

			}

		})
	}

}
