//go:build !ci
// +build !ci

package archiver

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestArchiverClient_getTickData(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 24889941)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Len(t, tickData.GetTransactionIds(), 10)
}

func TestArchiverClient_getTickData_givenEmptyTick(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 24800000)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.Nil(t, tickData)
	assert.Empty(t, tickData.GetTransactionIds())
}

func TestArchiverClient_getTickData_givenEmptyTickWithoutTransactions(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 24800003)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Empty(t, tickData.GetTransactionIds())
}
