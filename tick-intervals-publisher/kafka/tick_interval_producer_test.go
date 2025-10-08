package kafka

import (
	"encoding/binary"
	"testing"

	"github.com/qubic/tick-intervals-publisher/domain"
	"github.com/stretchr/testify/require"
)

func TestTickIntervalProducer_createRecord(t *testing.T) {
	tickInterval := &domain.TickInterval{
		Epoch: 1,
		From:  2,
		To:    3,
	}
	record, err := createRecord(tickInterval)
	require.NoError(t, err)
	require.NotNil(t, record)

	require.Equal(t, 1, int(binary.LittleEndian.Uint32(record.Key)))
	require.JSONEq(t, `{"epoch": 1, "from": 2, "to": 3}`, string(record.Value))
}
