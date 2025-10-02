//go:build !ci
// +build !ci

package archiver

import (
	"context"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

const url = "localhost:8001"

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

	tickData, err := client.GetTickData(context.Background(), 33717718)
	assert.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Greater(t, len(tickData.TransactionHashes), 1)
}

func TestArchiverClient_getTickData_givenEmptyTick(t *testing.T) {
	client, err := NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 33717719)
	assert.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.Nil(t, tickData)
}

// not easy to find. tick that isn't empty but has no transactions.
//func TestArchiverClient_getTransactions_givenTickWithoutTransactions(t *testing.T) {
//	client, err := NewClient(url)
//	assert.NoError(t, err)
//
//	tickData, err := client.GetTickData(context.Background(), 24800003)
//	assert.NoError(t, err)
//
//	log.Printf("Tick data: %+v", tickData)
//	assert.NotNil(t, tickData)
//	assert.Nil(t, tickData.TransactionHashes)
//	assert.Nil(t, tickData.ContractFees)
//}
