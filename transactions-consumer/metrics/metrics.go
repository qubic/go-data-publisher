package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	processedTickGauge    prometheus.Gauge
	processedMessageCount prometheus.Counter
	processedTicksCount   prometheus.Counter
}

func NewMetrics(namespace string) *Metrics {
	m := Metrics{
		// metrics for epoch, tick, message processing
		processedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_processed_tick", namespace),
			Help: "The latest fully processed tick",
		}),
		processedTicksCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_processed_tick_count", namespace),
			Help: "The total number of processed ticks",
		}),
		processedMessageCount: promauto.NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("%s_processed_message_count", namespace),
			Help: "The total number of processed message records",
		}),
	}
	return &m
}

func (metrics *Metrics) SetProcessedTick(tick uint32) {
	metrics.processedTickGauge.Set(float64(tick))
}

func (metrics *Metrics) IncProcessedTicks() {
	metrics.processedTicksCount.Inc()
}

func (metrics *Metrics) IncProcessedMessages() {
	metrics.processedMessageCount.Inc()
}
