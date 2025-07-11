package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ProcessingMetrics struct {
	sourceTickGauge       prometheus.Gauge
	sourceEpochGauge      prometheus.Gauge
	processedTickGauge    prometheus.Gauge
	processingEpochGauge  prometheus.Gauge
	processedMessageCount prometheus.Counter
}

func NewProcessingMetrics(namespace string) *ProcessingMetrics {
	m := ProcessingMetrics{
		// metrics for epoch, tick, event processing
		processedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_processed_tick", namespace),
			Help: "The latest fully processed tick",
		}),
		processingEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_processed_epoch", namespace),
			Help: "The current processing epoch",
		}),
		processedMessageCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_processed_message_count", namespace),
			Help: "The total number of processed message records",
		}),
		// metrics for comparison to event source
		sourceTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_source_tick", namespace),
			Help: "The latest known source tick",
		}),
		sourceEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_source_epoch", namespace),
			Help: "The latest known source epoch",
		}),
	}
	return &m
}

func (m *ProcessingMetrics) SetSourceTick(epoch uint32, tick uint32) {
	m.sourceEpochGauge.Set(float64(epoch))
	m.sourceTickGauge.Set(float64(tick))
}

func (m *ProcessingMetrics) SetProcessedTick(epoch uint32, tick uint32) {
	m.processingEpochGauge.Set(float64(epoch))
	m.processedTickGauge.Set(float64(tick))
}

func (m *ProcessingMetrics) IncProcessedMessages() {
	m.processedMessageCount.Inc()
}
