package domain

type Status struct {
	LatestEpoch   uint32
	LatestTick    uint32
	TickIntervals []*TickInterval
}

type TickInterval struct {
	Epoch uint32
	From  uint32
	To    uint32
}
