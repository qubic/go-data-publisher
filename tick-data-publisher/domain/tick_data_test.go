package domain

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTickData_marshalling(t *testing.T) {
	tickData := &TickData{
		ComputorIndex:     1,
		Epoch:             2,
		TickNumber:        3,
		Timestamp:         4,
		VarStruct:         "var-struct",
		TimeLock:          "time-lock",
		TransactionHashes: []string{"hash1", "hash2"},
		ContractFees:      []int64{1, 2, 3},
		Signature:         "signature",
	}

	expectedJson := `{"computorIndex":1,"epoch":2,"tickNumber":3,"timestamp":4,"varStruct":"var-struct","timeLock":"time-lock","transactionHashes":["hash1","hash2"],"contractFees":[1,2,3],"signature":"signature"}`
	marshalled, err := json.Marshal(tickData)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled TickData
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, tickData, &unmarshalled)
}

func TestTickData_marshalling_givenEmpty(t *testing.T) {
	tickData := &TickData{}

	expectedJson := `{"computorIndex":0,"epoch":0,"tickNumber":0,"timestamp":0}`
	marshalled, err := json.Marshal(tickData)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled TickData
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, tickData, &unmarshalled)
}
