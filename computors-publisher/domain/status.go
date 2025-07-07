package domain

type ProcessedTick struct {
	TickNumber uint32
	Epoch      uint32
}
type Status struct {
	LastProcessedTick ProcessedTick
	EpochList         []uint32
}
