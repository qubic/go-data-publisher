//go:build !ci
// +build !ci

package archiver

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const url = "localhost:8001"

func TestArchiverClient_getStatus(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	status, err := client.GetStatus(context.Background())
	assert.NoError(t, err)

	log.Printf("Status: %+v", status)
	assert.NotNil(t, status)
	assert.Greater(t, int(status.LastProcessedTick.TickNumber), 20000000)
	assert.Greater(t, int(status.LastProcessedTick.Epoch), 150)
}

func TestArchiverClient_getTickData(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 26028459)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Len(t, tickData.GetTransactionIds(), 3)
}

func TestArchiverClient_getTickData_givenEmptyTick(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 26028355)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.Nil(t, tickData)
	assert.Empty(t, tickData.GetTransactionIds())
}

func TestArchiverClient_getTickData_givenEmptyTickWithoutTransactions(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 26028357)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Empty(t, tickData.GetTransactionIds())
}
