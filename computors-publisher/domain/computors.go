package domain

type EpochComputors struct {
	Epoch      uint32   `json:"epoch"`
	Identities []string `json:"identities"`
	Signature  string   `json:"signature"` // hex -> base64
}
