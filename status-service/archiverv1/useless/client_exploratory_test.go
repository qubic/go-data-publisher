//go:build !ci
// +build !ci

package useless

import (
	"context"
	"log"
	"testing"

	"github.com/qubic/go-data-publisher/status-service/archiverv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const url = "localhost:8010"

func TestArchiverClient_getStatus(t *testing.T) {
	client, err := archiverv1.NewClient(url)
	assert.NoError(t, err)

	status, err := client.GetStatus(context.Background())
	assert.NoError(t, err)

	log.Printf("Status: %+v", status)
	assert.NotNil(t, status)
	assert.Greater(t, int(status.Tick), 20000000)
	assert.Greater(t, int(status.Epoch), 150)
}

func TestArchiverClient_getTickData(t *testing.T) {
	client, err := archiverv1.NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 34837151)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Len(t, tickData.GetTransactionIds(), 10)
}

func TestArchiverClient_getTickData_givenEmptyTick(t *testing.T) {
	client, err := archiverv1.NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 34837175)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.Nil(t, tickData)
	assert.Empty(t, tickData.GetTransactionIds())
}

func TestArchiverClient_getTickData_givenTickDataWithoutTransactions(t *testing.T) {
	client, err := archiverv1.NewClient(url)
	assert.NoError(t, err)

	tickData, err := client.GetTickData(context.Background(), 34411948)
	require.NoError(t, err)

	log.Printf("Tick data: %+v", tickData)
	assert.NotNil(t, tickData)
	assert.Empty(t, tickData.GetTransactionIds())
}
