package entities

type Tx struct {
	TxID       string `json:"transactionHash"`
	SourceID   string `json:"source"`
	DestID     string `json:"destination"`
	Amount     int64  `json:"amount"`
	TickNumber uint32 `json:"tickNumber"`
	InputType  uint32 `json:"inputType"`
	InputSize  uint32 `json:"inputSize"`
	Input      string `json:"inputData"`
	Signature  string `json:"signature"`
	Timestamp  uint64 `json:"timestamp"`
	MoneyFlew  bool   `json:"moneyFlew"`
}

type ProcessedTickIntervalsPerEpoch struct {
	Epoch     uint32
	Intervals []ProcessedTickInterval
}

type ProcessedTickInterval struct {
	InitialProcessedTick uint32
	LastProcessedTick    uint32
}
