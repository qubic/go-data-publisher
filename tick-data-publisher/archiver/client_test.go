package archiver

import (
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/tick-data-publisher/domain"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArchiverClient_convertTickData(t *testing.T) {
	td := &protobuff.TickData{}
	data, err := convertTickData(td)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Empty(t, data)
	assert.Equal(t, &domain.TickData{}, data)
	assert.Nil(t, data.TransactionHashes)
	assert.Nil(t, data.ContractFees)
}
