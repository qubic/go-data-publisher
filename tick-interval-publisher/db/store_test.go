package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPebbleStore_SetAndGetLastProcessedEpoch(t *testing.T) {

	testDir := t.TempDir()

	store, err := NewPebbleStore(testDir)
	require.NoError(t, err)
	defer func(store *PebbleStore) {
		_ = store.Close()
	}(store)

	epoch := uint32(150)
	err = store.SetLastProcessedEpoch(epoch)
	require.NoError(t, err)

	retrieved, err := store.GetLastProcessedEpoch()
	require.NoError(t, err)
	require.Equal(t, epoch, retrieved)

}

func TestPebbleStore_GetLastProcessedEpochNotSet(t *testing.T) {

	testDir := t.TempDir()

	store, err := NewPebbleStore(testDir)
	require.NoError(t, err)
	defer func(store *PebbleStore) {
		_ = store.Close()
	}(store)

	_, err = store.GetLastProcessedEpoch()
	require.Error(t, err)
	require.Equal(t, ErrNotFound, err)
}

func TestPebbleStore_UpdateLastProcessedEpoch(t *testing.T) {

	testDir := t.TempDir()

	store, err := NewPebbleStore(testDir)
	require.NoError(t, err)
	defer func(store *PebbleStore) {
		_ = store.Close()
	}(store)

	initialEpoch := uint32(150)
	err = store.SetLastProcessedEpoch(initialEpoch)
	require.NoError(t, err)

	retrieved, err := store.GetLastProcessedEpoch()
	require.NoError(t, err)
	require.Equal(t, initialEpoch, retrieved)

	updatedEpoch := uint32(160)
	err = store.SetLastProcessedEpoch(updatedEpoch)
	require.NoError(t, err)

	retrieved, err = store.GetLastProcessedEpoch()
	require.NoError(t, err)
	require.Equal(t, updatedEpoch, retrieved)
}
