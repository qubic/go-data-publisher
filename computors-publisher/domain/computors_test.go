package domain

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEpochComputorsMarshallingEmpty(t *testing.T) {

	epochComputors := EpochComputors{}

	expectedJson := `{"epoch":0,"identities":null,"signature":""}`

	marshalled, err := json.Marshal(epochComputors)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled EpochComputors
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, epochComputors, unmarshalled)

}
