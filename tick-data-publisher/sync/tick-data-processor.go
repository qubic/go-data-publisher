package sync

import (
	"context"
	"github.com/qubic/tick-data-publisher/archiver"
	"github.com/qubic/tick-data-publisher/metrics"
	"log"
	"time"
)

type ArchiveClient interface {
	GetStatus(ctx context.Context) (*archiver.Status, error)
	GetTickData(ctx context.Context, tickNumber uint32) ([]string, error)
}

type DataStore interface {
	SetLastProcessedTick(tick uint32) error
	GetLastProcessedTick() (tick uint32, err error)
}

type TickDataProcessor struct {
	archiveClient     ArchiveClient
	dataStore         DataStore
	processingMetrics *metrics.ProcessingMetrics
}

func NewTickDataProcessor(db DataStore, client ArchiveClient, m *metrics.ProcessingMetrics) *TickDataProcessor {
	tdp := TickDataProcessor{
		dataStore:         db,
		archiveClient:     client,
		processingMetrics: m,
	}
	return &tdp
}

func (p *TickDataProcessor) StartProcessing() {

	ticker := time.Tick(1 * time.Second)
	for range ticker {
		log.Print("tick...")
	}

}
