package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	sourceTickGauge                  prometheus.Gauge
	sourceEpochGauge                 prometheus.Gauge
	processedTransactionsTickGauge   prometheus.Gauge
	processingTransactionsEpochGauge prometheus.Gauge
}

func NewMetrics(namespace string) *Metrics {
	m := Metrics{
		// metrics for epoch, tick, message processing
		processedTransactionsTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_processed_transactions_tick", namespace),
			Help: "The latest fully processed tick of transactions",
		}),
		processingTransactionsEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_processed_transactions_epoch", namespace),
			Help: "The current processing epoch",
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

func (metrics *Metrics) SetProcessedTransactionsTick(epoch uint32, tick uint32) {
	metrics.processingTransactionsEpochGauge.Set(float64(epoch))
	metrics.processedTransactionsTickGauge.Set(float64(tick))
}

func (metrics *Metrics) SetSourceTick(epoch uint32, tick uint32) {
	metrics.sourceEpochGauge.Set(float64(epoch))
	metrics.sourceTickGauge.Set(float64(tick))
}
