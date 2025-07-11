package domain

type EpochComputors struct {
	Epoch      uint32   `json:"epoch"`
	TickNumber uint32   `json:"tickNumber"`
	Identities []string `json:"identities"`
	Signature  string   `json:"signature"` // hex -> base64
}
