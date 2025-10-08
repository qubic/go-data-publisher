package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEpochComputorsMarshallingEmpty(t *testing.T) {

	epochComputors := EpochComputors{}

	expectedJson := `{"epoch":0,"tickNumber":0,"identities":null,"signature":""}`

	marshalled, err := json.Marshal(epochComputors)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled EpochComputors
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, epochComputors, unmarshalled)

}

func TestEpochComputorsMarshalling(t *testing.T) {

	epochComputors := EpochComputors{
		Epoch:      100,
		TickNumber: 10000000,
		Identities: []string{
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		},
		Signature: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	expectedJson := `{"epoch":100,"tickNumber":10000000,"identities":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],"signature":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`

	marshalled, err := json.Marshal(epochComputors)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled EpochComputors
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, epochComputors, unmarshalled)

}
