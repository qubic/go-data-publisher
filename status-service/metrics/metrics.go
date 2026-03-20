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

	logsLastProcessedTickGauge prometheus.Gauge
	logsErrorGauge             prometheus.Gauge
	logsRedisLastIngestedTick  prometheus.Gauge
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
		// metrics for comparison to source
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

		// Logs processing related metrics
		logsLastProcessedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "logs",
			Name:      "last_processed_tick",
			Help:      "Last processed logs tick",
		}),
		logsErrorGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "logs",
			Name:      "errors_gauge",
			Help:      "Number of occurred errors. Resets upon successful processing of a tick",
		}),
		logsRedisLastIngestedTick: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "logs",
			Name:      "redis_last_ingested_tick",
			Help:      "Last ingested tick reported by redis.",
		}),
	}
	return &m
}

func (m *Metrics) SetProcessedTransactionsTick(epoch uint32, tick uint32) {
	m.lastProcessedTick = tick
	m.processingTransactionsEpochGauge.Set(float64(epoch))
	m.processedTransactionsTickGauge.Set(float64(tick))
}

func (m *Metrics) SetSourceTick(epoch uint32, tick uint32) {
	m.sourceEpochGauge.Set(float64(epoch))
	m.sourceTickGauge.Set(float64(tick))
}

func (m *Metrics) SetError(count uint) {
	m.hasErrorGauge.Set(float64(count))
}

func (m *Metrics) GetLastProcessedTick() uint32 {
	return m.lastProcessedTick
}

func (m *Metrics) SetLogsLastProcessedTick(tickNumber uint32) {
	m.logsLastProcessedTickGauge.Set(float64(tickNumber))
}

func (m *Metrics) SetLogsErrors(count uint) {
	m.logsErrorGauge.Set(float64(count))
}

func (m *Metrics) SetLogsRedisLastIngestedTick(tickNumber uint32) {
	m.logsRedisLastIngestedTick.Set(float64(tickNumber))
}
