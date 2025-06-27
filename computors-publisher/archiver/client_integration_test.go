//go:build !ci
// +build !ci

package archiver

import (
	"context"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

const url = "localhost:8010"

func TestClient_GetStatus(t *testing.T) {

	client, err := NewClient(url)
	assert.NoError(t, err)

	status, err := client.GetStatus(context.Background())
	assert.NoError(t, err)

	log.Printf("Status: %+v", status)
	assert.NotNil(t, status)
	assert.NotEmpty(t, status.EpochList)
}

func TestClient_GetEpochComputors(t *testing.T) {

	client, err := NewClient(url)
	assert.NoError(t, err)

	epochComputors, err := client.GetEpochComputors(context.Background(), 150)
	assert.NoError(t, err)
	assert.NotNil(t, epochComputors)

	assert.Equal(t, uint32(150), epochComputors.Epoch)
	assert.Equal(t, 676, len(epochComputors.Identities))
	assert.Equal(t, "BPHYQKCKUIWYQBQENTYNWZODLAIALNVOSNSJRGZQQCECHTAGPQFZQNICYTZL", epochComputors.Identities[6])
	assert.Equal(t, "3hNcxJqgBcN49ZKwfjBcYU/j+bFc4kgX2fqemdKInT4Gtlzx9CB8hg/FIpw/nMVVcVPDmQMBbBqhj4MGLEwXAA==", epochComputors.Signature)

}
