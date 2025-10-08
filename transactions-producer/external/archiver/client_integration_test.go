//go:build !ci
// +build !ci

package archiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const url = "localhost:8001"

func TestArchiverClient_getTickTransactions(t *testing.T) {
	client, err := NewClient(url)
	require.NoError(t, err)
	transactions, err := client.GetTickTransactions(context.Background(), 33717826)
	require.NoError(t, err)
	require.NotEmpty(t, transactions)
}

func TestArchiverClient_getTickTransactions_emptyTick(t *testing.T) {
	client, err := NewClient(url)
	require.NoError(t, err)
	transactions, err := client.GetTickTransactions(context.Background(), 33717827)
	require.NoError(t, err)
	require.Empty(t, transactions)
}

func TestArchiverClient_getTickTransactions_invalidTick(t *testing.T) {
	client, err := NewClient(url)
	require.NoError(t, err)
	_, err = client.GetTickTransactions(context.Background(), 666)
	require.Error(t, err)
}
