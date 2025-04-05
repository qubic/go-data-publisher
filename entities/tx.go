package entities

type Tx struct {
	TxID       string
	SourceID   string
	DestID     string
	Amount     int64
	TickNumber uint32
	InputType  uint32
	InputSize  uint32
	Input      string
	Signature  string
	Timestamp  uint64
	MoneyFlew  bool
}

type ProcessedTickIntervalsPerEpoch struct {
	Epoch     uint32
	Intervals []ProcessedTickInterval
}

type ProcessedTickInterval struct {
	InitialProcessedTick uint32
	LastProcessedTick    uint32
}
