package domain

type Status struct {
	LatestEpoch   uint32
	LatestTick    uint32
	TickIntervals []*TickInterval
}
