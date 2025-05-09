package domain

type TickData struct {
	ComputorIndex     uint32   `json:"computorIndex"`
	Epoch             uint32   `json:"epoch"`
	TickNumber        uint32   `json:"tickNumber"`
	Timestamp         uint64   `json:"timestamp"`
	VarStruct         string   `json:"varStruct,omitempty"` // []byte -> base64
	TimeLock          string   `json:"timeLock,omitempty"`  // []byte -> base64
	TransactionHashes []string `json:"transactionHashes,omitempty"`
	ContractFees      []int64  `json:"contractFees,omitempty"`
	Signature         string   `json:"signature,omitempty"` // hex -> base64
}
