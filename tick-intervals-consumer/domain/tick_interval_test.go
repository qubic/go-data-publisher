package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTickInterval_convertFromJson(t *testing.T) {
	jsonString := `{ "epoch":42, "from":123, "to":456 }`

	interval := TickInterval{}
	err := json.Unmarshal([]byte(jsonString), &interval)
	require.NoError(t, err)

	assert.Equal(t, 42, int(interval.Epoch))
	assert.Equal(t, 123, int(interval.From))
	assert.Equal(t, 456, int(interval.To))
}

func TestTickInterval_convertToJson(t *testing.T) {
	interval := TickInterval{
		Epoch: 42,
		From:  123,
		To:    456,
	}

	val, err := json.Marshal(interval)
	require.NoError(t, err)

	require.JSONEq(t, `{"epoch":42, "from":123, "to":456}`, string(val))
}
