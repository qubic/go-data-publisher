package db

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPebbleStore_SetAndGetLastProcessedEpoch(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	epoch := uint32(150)
	err = store.SetLastProcessedEpoch(epoch)
	assert.NoError(t, err)

	retrieved, err := store.GetLastProcessedEpoch()
	assert.NoError(t, err)
	assert.Equal(t, epoch, retrieved)

}

func TestPebbleStore_GetLastProcessedEpochNotSet(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	_, err = store.GetLastProcessedEpoch()
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestPebbleStore_UpdateLastProcessedEpoch(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "processor_store_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	initialEpoch := uint32(150)
	updatedEpoch := uint32(160)

	store, err := NewPebbleStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	err = store.SetLastProcessedEpoch(initialEpoch)
	assert.NoError(t, err)

	retrieved, err := store.GetLastProcessedEpoch()
	assert.NoError(t, err)
	assert.Equal(t, initialEpoch, retrieved)

	err = store.SetLastProcessedEpoch(updatedEpoch)
	assert.NoError(t, err)

	retrieved, err = store.GetLastProcessedEpoch()
	assert.NoError(t, err)
	assert.Equal(t, updatedEpoch, retrieved)

}
