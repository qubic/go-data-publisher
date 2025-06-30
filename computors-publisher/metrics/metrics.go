package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
)

type ProcessingMetrics struct {
	processingEpochGauge prometheus.Gauge
}

func NewProcessingMetrics(namespace string) *ProcessingMetrics {
	return &ProcessingMetrics{
		processingEpochGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_processing_epoch", namespace),
		}),
	}
}

func (m *ProcessingMetrics) SetProcessedEpoch(epoch uint32) {
	m.processingEpochGauge.Set(float64(epoch))
}
