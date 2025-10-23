package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusServiceServer_getStatus(t *testing.T) {
	server := StatusServiceServer{
		statusCache: &StatusService{
			database: &FakeStatusProvider{
				lastProcessedTick:  42,
				lastProcessedEpoch: 43,
			},
		},
	}

	response, err := server.GetStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 42, int(response.LastProcessedTick))
	require.Equal(t, 43, int(response.LastProcessedEpoch))
}
