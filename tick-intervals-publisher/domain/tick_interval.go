package domain

type TickInterval struct {
	Epoch uint32 `json:"epoch"`
	From  uint32 `json:"from"`
	To    uint32 `json:"to"`
}
