package db

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestPebbleStore_SetAndGetLastProcessedEpoch(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	epoch := uint32(150)
	err = store.SetLastProcessedEpoch(epoch)
	require.NoError(t, err)

	retrieved, err := store.GetLastProcessedEpoch()
	require.NoError(t, err)
	require.Equal(t, epoch, retrieved)

}

func TestPebbleStore_GetLastProcessedEpochNotSet(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.GetLastProcessedEpoch()
	require.Error(t, err)
	require.Equal(t, ErrNotFound, err)
}

func TestPebbleStore_UpdateLastProcessedEpoch(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	initialEpoch := uint32(150)
	updatedEpoch := uint32(160)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	err = store.SetLastProcessedEpoch(initialEpoch)
	require.NoError(t, err)

	retrieved, err := store.GetLastProcessedEpoch()
	require.NoError(t, err)
	require.Equal(t, initialEpoch, retrieved)

	err = store.SetLastProcessedEpoch(updatedEpoch)
	require.NoError(t, err)

	retrieved, err = store.GetLastProcessedEpoch()
	require.NoError(t, err)
	require.Equal(t, updatedEpoch, retrieved)

}

func TestPebbleStore_SetAndGetLastStoredComputorListSum(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	sum := []byte("sum")
	epoch := uint32(162)
	err = store.SetLastStoredComputorListSum(epoch, sum)
	require.NoError(t, err)

	err = store.SetLastStoredComputorListSum(uint32(123), []byte("foo"))
	require.NoError(t, err)

	retrieved, err := store.GetLastStoredComputorListSum(epoch)
	require.NoError(t, err)
	require.Equal(t, sum, retrieved)

}
