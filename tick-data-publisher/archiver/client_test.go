package archiver

import (
	"testing"

	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/tick-data-publisher/domain"
	"github.com/stretchr/testify/assert"
)

func TestArchiverClient_convertTickData(t *testing.T) {
	td := &protobuf.TickData{}
	data, err := convertTickData(td)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Empty(t, data)
	assert.Equal(t, &domain.TickData{}, data)
	assert.Nil(t, data.TransactionHashes)
	assert.Nil(t, data.ContractFees)
}
