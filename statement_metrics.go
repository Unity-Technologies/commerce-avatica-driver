package avatica

import "sync/atomic"

// StatementLifecycleCounters tracks statement lifecycle operations for leak detection.
type StatementLifecycleCounters struct {
	Opened         uint64
	CloseSucceeded uint64
	CloseFailed    uint64
}

var (
	statementOpenedCounter         atomic.Uint64
	statementCloseSucceededCounter atomic.Uint64
	statementCloseFailedCounter    atomic.Uint64
)

// StatementLifecycleStats returns cumulative process-wide statement lifecycle counters.
func StatementLifecycleStats() StatementLifecycleCounters {
	return StatementLifecycleCounters{
		Opened:         statementOpenedCounter.Load(),
		CloseSucceeded: statementCloseSucceededCounter.Load(),
		CloseFailed:    statementCloseFailedCounter.Load(),
	}
}

func recordStatementOpened() {
	statementOpenedCounter.Add(1)
}

func recordStatementCloseSucceeded() {
	statementCloseSucceededCounter.Add(1)
}

func recordStatementCloseFailed() {
	statementCloseFailedCounter.Add(1)
}

func resetStatementLifecycleStats() {
	statementOpenedCounter.Store(0)
	statementCloseSucceededCounter.Store(0)
	statementCloseFailedCounter.Store(0)
}
