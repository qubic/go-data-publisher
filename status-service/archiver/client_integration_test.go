//go:build !ci
// +build !ci

package archiver

import (
	"context"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

const url = "localhost:8010"

func TestArchiverClient_getStatus(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	status, err := client.GetStatus(context.Background())
	assert.NoError(t, err)

	log.Printf("Status: %+v", status)
	assert.NotNil(t, status)
	assert.Greater(t, int(status.LatestTick), 20000000)
	assert.Greater(t, int(status.LatestEpoch), 150)
	assert.NotEmpty(t, status.TickIntervals)
}

func TestArchiverClient_getTransactions(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	hashes, err := client.GetTickData(context.Background(), 24889941)
	assert.NoError(t, err)

	log.Printf("Tick transactions: %+v", hashes)
	assert.NotNil(t, hashes)
	assert.Len(t, hashes, 10)
}

func TestArchiverClient_getTransactions_givenEmptyTick(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	hashes, err := client.GetTickData(context.Background(), 24800000)
	assert.NoError(t, err)

	log.Printf("Tick transactions: %+v", hashes)
	assert.NotNil(t, hashes)
	assert.Empty(t, hashes)
}

func TestArchiverClient_getTransactions_givenEmptyTickWithoutTransactions(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	hashes, err := client.GetTickData(context.Background(), 24800003)
	assert.NoError(t, err)

	log.Printf("Tick transactions: %+v", hashes)
	assert.NotNil(t, hashes)
	assert.Empty(t, hashes)
}
