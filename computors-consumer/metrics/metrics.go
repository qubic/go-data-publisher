package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	processedTickGauge    prometheus.Gauge
	processedMessageCount prometheus.Counter
	processingEpochGauge  prometheus.Gauge
}

func NewMetrics(namespace string) *Metrics {
	m := Metrics{
		// metrics for epoch, tick, message processing
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
	}
	return &m
}

func (metrics *Metrics) SetProcessedTick(epoch uint32, tick uint32) {
	metrics.processingEpochGauge.Set(float64(epoch))
	metrics.processedTickGauge.Set(float64(tick))
}

func (metrics *Metrics) IncProcessedMessages(count int) {
	metrics.processedMessageCount.Add(float64(count))
}
