package domain

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	sourceTickGauge       prometheus.Gauge
	sourceEpochGauge      prometheus.Gauge
	processedTickGauge    prometheus.Gauge
	processingEpochGauge  prometheus.Gauge
	processedMessageCount prometheus.Counter
	processedTicksCount   prometheus.Counter
}

func NewMetrics(namespace string) *Metrics {
	m := Metrics{
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

func (metrics *Metrics) SetProcessedTick(epoch uint32, tick uint32) {
	metrics.processingEpochGauge.Set(float64(epoch))
	metrics.processedTickGauge.Set(float64(tick))
}

func (metrics *Metrics) IncProcessedTicks(count int) {
	metrics.processedTicksCount.Add(float64(count))
}

func (metrics *Metrics) IncProcessedMessages(count int) {
	metrics.processedMessageCount.Add(float64(count))
}

func (metrics *Metrics) SetSourceTick(epoch uint32, tick uint32) {
	metrics.sourceEpochGauge.Set(float64(epoch))
	metrics.sourceTickGauge.Set(float64(tick))
}
