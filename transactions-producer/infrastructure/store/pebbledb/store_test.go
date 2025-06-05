package pebbledb

import (
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
			expected: 10000000,
		},
		{
			name:     "TestLastProcessedTick_2",
			expected: 20048336,
		},
		{
			name:     "TestLastProcessedTick_3",
			expected: 30216726,
		},
		{
			name:     "TestLastProcessedTick_4",
			expected: 60326734,
		},
		{
			name:     "TestLastProcessedTick_5",
			expected: 10048735,
		},
	}

	for _, testRun := range testData {
		t.Run(testRun.name, func(t *testing.T) {
			err := store.SetLastProcessedTick(testRun.expected)
			require.NoError(t, err)

			got, err := store.GetLastProcessedTick()
			require.NoError(t, err)
			require.Equal(t, testRun.expected, got)
		})
	}

}
