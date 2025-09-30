package domain

import (
	"github.com/pkg/errors"
	archiverproto "github.com/qubic/go-archiver-v2/protobuf"
)

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

func ConvertFromArchiverStatus(archiverStatus *archiverproto.GetStatusResponse) (*Status, error) {
	var intervals []*TickInterval
	epochs := archiverStatus.GetProcessedTickIntervalsPerEpoch()
	for _, epochIntervals := range epochs {
		for _, interval := range epochIntervals.Intervals {
			intervals = append(intervals, &TickInterval{
				Epoch: epochIntervals.Epoch,
				From:  interval.InitialProcessedTick,
				To:    interval.LastProcessedTick,
			})
		}
	}

	initialTick, err := calculateInitialTickOfCurrentEpoch(epochs)
	if err != nil {
		return nil, err
	}

	status := Status{
		Tick:          archiverStatus.GetLastProcessedTick().GetTickNumber(),
		Epoch:         archiverStatus.GetLastProcessedTick().GetEpoch(),
		InitialTick:   initialTick,
		TickIntervals: intervals,
	}
	return &status, nil
}

func calculateInitialTickOfCurrentEpoch(epochs []*archiverproto.ProcessedTickIntervalsPerEpoch) (uint32, error) {
	numberOfEpochs := len(epochs)
	if numberOfEpochs > 0 {
		latestEpoch := epochs[numberOfEpochs-1]
		if len(latestEpoch.GetIntervals()) > 0 {
			return latestEpoch.Intervals[0].InitialProcessedTick, nil
		}
	}
	return 0, errors.New("calculating initial tick")
}
