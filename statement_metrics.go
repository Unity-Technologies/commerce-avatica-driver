package avatica

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// StatementLifecycleCounters tracks statement lifecycle operations for leak detection.
type StatementLifecycleCounters struct {
	Opened              uint64
	CloseSucceeded      uint64
	CloseFailed         uint64
	InFlight            uint64
	ConnectionDiscarded uint64
}

var (
	statementOpenedCounter         atomic.Uint64
	statementCloseSucceededCounter atomic.Uint64
	statementCloseFailedCounter    atomic.Uint64
	statementInFlightCounter       atomic.Uint64
	connectionDiscardedCounter     atomic.Uint64
)

// StatementLifecycleStats returns cumulative process-wide statement lifecycle counters.
func StatementLifecycleStats() StatementLifecycleCounters {
	return StatementLifecycleCounters{
		Opened:              statementOpenedCounter.Load(),
		CloseSucceeded:      statementCloseSucceededCounter.Load(),
		CloseFailed:         statementCloseFailedCounter.Load(),
		InFlight:            statementInFlightCounter.Load(),
		ConnectionDiscarded: connectionDiscardedCounter.Load(),
	}
}

// RegisterStatementLifecycleCollector registers driver-level statement metrics with Prometheus.
// namespace and subsystem are optional and follow Prometheus metric naming conventions.
func RegisterStatementLifecycleCollector(reg prometheus.Registerer, namespace, subsystem string) error {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	collector := newStatementLifecycleCollector(namespace, subsystem)

	if err := reg.Register(collector); err != nil {
		if alreadyRegisteredErr, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if _, ok := alreadyRegisteredErr.ExistingCollector.(*statementLifecycleCollector); ok {
				return nil
			}
		}
		return err
	}

	return nil
}

func recordStatementOpened() {
	statementOpenedCounter.Add(1)
	statementInFlightCounter.Add(1)
}

func recordStatementCloseSucceeded() {
	statementCloseSucceededCounter.Add(1)
	decrementInFlightCounter()
}

func recordStatementCloseFailed() {
	statementCloseFailedCounter.Add(1)
	decrementInFlightCounter()
}

func recordConnectionDiscarded() {
	connectionDiscardedCounter.Add(1)
}

func decrementInFlightCounter() {
	for {
		current := statementInFlightCounter.Load()
		if current == 0 {
			return
		}
		if statementInFlightCounter.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func resetStatementLifecycleStats() {
	statementOpenedCounter.Store(0)
	statementCloseSucceededCounter.Store(0)
	statementCloseFailedCounter.Store(0)
	statementInFlightCounter.Store(0)
	connectionDiscardedCounter.Store(0)
}

type statementLifecycleCollector struct {
	openedDesc    *prometheus.Desc
	closedDesc    *prometheus.Desc
	failedDesc    *prometheus.Desc
	inUseDesc     *prometheus.Desc
	discardedDesc *prometheus.Desc
}

func newStatementLifecycleCollector(namespace, subsystem string) *statementLifecycleCollector {
	return &statementLifecycleCollector{
		openedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "statement_opened_total"),
			"Total number of statements opened by the avatica driver",
			nil,
			nil,
		),
		closedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "statement_close_succeeded_total"),
			"Total number of statements successfully closed by the avatica driver",
			nil,
			nil,
		),
		failedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "statement_close_failed_total"),
			"Total number of statement close failures in the avatica driver",
			nil,
			nil,
		),
		inUseDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "statement_in_flight"),
			"Current number of statements opened minus close attempts in the avatica driver",
			nil,
			nil,
		),
		discardedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "connection_discarded_total"),
			"Total number of connections discarded due to degraded state (e.g. statement limit exceeded)",
			nil,
			nil,
		),
	}
}

func (c *statementLifecycleCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.openedDesc
	ch <- c.closedDesc
	ch <- c.failedDesc
	ch <- c.inUseDesc
	ch <- c.discardedDesc
}

func (c *statementLifecycleCollector) Collect(ch chan<- prometheus.Metric) {
	stats := StatementLifecycleStats()
	ch <- prometheus.MustNewConstMetric(c.openedDesc, prometheus.CounterValue, float64(stats.Opened))
	ch <- prometheus.MustNewConstMetric(c.closedDesc, prometheus.CounterValue, float64(stats.CloseSucceeded))
	ch <- prometheus.MustNewConstMetric(c.failedDesc, prometheus.CounterValue, float64(stats.CloseFailed))
	ch <- prometheus.MustNewConstMetric(c.inUseDesc, prometheus.GaugeValue, float64(stats.InFlight))
	ch <- prometheus.MustNewConstMetric(c.discardedDesc, prometheus.CounterValue, float64(stats.ConnectionDiscarded))
}
