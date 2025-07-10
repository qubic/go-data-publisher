package sync

import (
	"encoding/hex"
	"github.com/qubic/computors-publisher/domain"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProcessor_computeComputorsChecksum(t *testing.T) {

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

	sum, err := computeComputorsChecksum(data)
	assert.NoError(t, err)
	assert.Equal(t, "846e6cd5a26cd76c361d802bcf12d4c4eb02cf66268ebff18af9e921dae0118f", hex.EncodeToString(sum))
}
