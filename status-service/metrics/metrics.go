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
	hasErrorGauge                    prometheus.Gauge
	lastProcessedTick                uint32
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
		hasErrorGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_has_error", namespace),
			Help: "Number of subsequent processing errors",
		}),
	}
	return &m
}

func (metrics *Metrics) SetProcessedTransactionsTick(epoch uint32, tick uint32) {
	metrics.lastProcessedTick = tick
	metrics.processingTransactionsEpochGauge.Set(float64(epoch))
	metrics.processedTransactionsTickGauge.Set(float64(tick))
}

func (metrics *Metrics) SetSourceTick(epoch uint32, tick uint32) {
	metrics.sourceEpochGauge.Set(float64(epoch))
	metrics.sourceTickGauge.Set(float64(tick))
}

func (metrics *Metrics) SetError(count uint) {
	metrics.hasErrorGauge.Set(float64(count))
}

func (metrics *Metrics) GetLastProcessedTick() uint32 {
	return metrics.lastProcessedTick
}
