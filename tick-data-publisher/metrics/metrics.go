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
	processedTicksCount   prometheus.Counter
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
		processedTicksCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_processed_tick_count", namespace),
			Help: "The total number of processed ticks",
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

func (metrics *ProcessingMetrics) SetProcessedTick(epoch uint32, tick uint32) {
	metrics.processingEpochGauge.Set(float64(epoch))
	metrics.processedTickGauge.Set(float64(tick))
}

func (metrics *ProcessingMetrics) IncProcessedTicks() {
	metrics.processedTicksCount.Inc()
}

func (metrics *ProcessingMetrics) AddProcessedMessages(count int) {
	metrics.processedMessageCount.Add(float64(count))
}

func (metrics *ProcessingMetrics) SetSourceTick(epoch uint32, tick uint32) {
	metrics.sourceEpochGauge.Set(float64(epoch))
	metrics.sourceTickGauge.Set(float64(tick))
}
