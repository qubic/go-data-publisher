package domain

type TickData struct {
	ComputorIndex  uint32
	Epoch          uint32
	TickNumber     uint32
	Timestamp      uint64
	VarStruct      []byte
	TimeLock       []byte
	TransactionIds []string
	ContractFees   []int64
	Signature      string
}

// GetTransactionIds to support call on nil object
func (x *TickData) GetTransactionIds() []string {
	if x != nil {
		return x.TransactionIds
	}
	return nil
}
