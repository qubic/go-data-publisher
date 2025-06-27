package archiver

import (
	"github.com/qubic/computors-publisher/domain"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArchiveClient_convertComputorList(t *testing.T) {
	computorList := &protobuff.Computors{}

	data, err := convertComputorList(computorList)
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Empty(t, data)
	assert.Equal(t, &domain.EpochComputors{}, data)
	assert.Equal(t, uint32(0), data.Epoch)
	assert.Nil(t, data.Identities)
	assert.Equal(t, "", data.Signature)
}
