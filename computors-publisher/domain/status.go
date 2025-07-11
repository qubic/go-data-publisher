package domain

type ProcessedTick struct {
	TickNumber uint32
	Epoch      uint32
}

type TickInterval struct {
	FirstTick uint32
	LastTick  uint32
}

type Status struct {
	LastProcessedTick ProcessedTick
	EpochList         []uint32
	TickIntervals     map[uint32][]*TickInterval
}
