package domain

type Status struct {
	Epoch         uint32
	Tick          uint32
	InitialTick   uint32
	TickIntervals []*TickInterval
}

type TickInterval struct {
	Epoch uint32
	From  uint32
	To    uint32
}
