package domain

type Status struct {
	LatestEpoch   uint32
	LatestTick    uint32
	TickIntervals []*TickInterval
}

type TickInterval struct {
	Epoch uint32 `json:"epoch"`
	From  uint32 `json:"from"`
	To    uint32 `json:"to"`
}
