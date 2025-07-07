package sync

import (
	"encoding/hex"
	"github.com/qubic/computors-publisher/domain"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProcessor_computeComputorListSum(t *testing.T) {

	data := domain.EpochComputors{
		Epoch:      100,
		TickNumber: 0,
		Identities: []string{
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		},
		Signature: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}

	sum, err := computeComputorListSum(data)
	assert.NoError(t, err)
	assert.Equal(t, "21a9ba302828d7956e9362111378b3cc", hex.EncodeToString(sum))
}
