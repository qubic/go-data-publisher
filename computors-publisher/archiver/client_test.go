package archiver

import (
	"github.com/qubic/computors-publisher/domain"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"testing"
)

func TestArchiveClient_convertComputorList(t *testing.T) {
	computorList := &protobuff.Computors{
		Epoch: 150,
		Identities: []string{
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		},
		SignatureHex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	data, err := convertComputorList(computorList)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.NotEmpty(t, data)
	assert.Equal(t, &domain.EpochComputors{
		Epoch: 150,
		Identities: []string{
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		},
		Signature: "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqg==",
	}, data)
	assert.Equal(t, uint32(150), data.Epoch)
	assert.NotNil(t, data.Identities)
	assert.Equal(t, "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqg==", data.Signature)
}

func TestArchiveClient_convertStatusResponse(t *testing.T) {

	content, err := os.ReadFile("testdata/example-status-response.json")
	require.NoError(t, err)

	var statusResponse protobuff.GetStatusResponse
	err = protojson.Unmarshal(content, &statusResponse)
	require.NoError(t, err)

	status := convertStatus(&statusResponse)
	require.NotNil(t, status)
	require.NotNil(t, status.LastProcessedTick)
	assert.Equal(t, 168, int(status.LastProcessedTick.Epoch))
	assert.Equal(t, 28961006, int(status.LastProcessedTick.TickNumber))

	// test data starts from epoch 160. current epoch is 168.
	assert.Equal(t, 9, len(status.TickIntervals))
	assert.Len(t, status.TickIntervals[160], 1)
	assert.Len(t, status.TickIntervals[168], 2)
	assert.Equal(t, 28375000, int(status.TickIntervals[168][0].FirstTick))
	assert.Equal(t, 28375232, int(status.TickIntervals[168][0].LastTick))
	assert.Equal(t, 28376000, int(status.TickIntervals[168][1].FirstTick))
	assert.Equal(t, 28961006, int(status.TickIntervals[168][1].LastTick))

	// check epoch list
	assert.Subset(t, status.EpochList, []uint32{160, 161, 162, 163, 164, 165, 166, 167, 168})

}
