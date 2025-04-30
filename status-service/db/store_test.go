package db

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestStore_SetAndGetLastProcessedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	var tick uint32 = 123
	err = store.SetLastProcessedTick(tick)
	assert.NoError(t, err)

	retrievedTick, err := store.GetLastProcessedTick()
	assert.NoError(t, err)
	assert.Equal(t, tick, retrievedTick)
}

func TestStore_GetLastProcessedTickNotSet(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	_, err = store.GetLastProcessedTick()
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestStore_UpdateLastProcessedTick(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	store, err := NewPebbleStore(tempDir)
	assert.NoError(t, err)
	defer store.Close()

	var initialTick uint32 = 456
	var newTick uint32 = 789

	err = store.SetLastProcessedTick(initialTick)
	assert.NoError(t, err)

	retrievedTick, err := store.GetLastProcessedTick()
	assert.NoError(t, err)
	assert.Equal(t, initialTick, retrievedTick)

	err = store.SetLastProcessedTick(newTick)
	assert.NoError(t, err)

	retrievedTick, err = store.GetLastProcessedTick()
	assert.NoError(t, err)
	assert.Equal(t, newTick, retrievedTick)
}
