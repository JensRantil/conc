package conc

import (
	"sync/atomic"
	"time"
)

type NonBlockingReporter struct {
	latencies  chan Execution
	noWorkChan chan struct{}
	inFlight   int32
}

func NewNonBlockingReporter(chanSize int) *NonBlockingReporter {
	return &NonBlockingReporter{
		latencies:  make(chan Execution, chanSize),
		noWorkChan: make(chan struct{}),
	}
}

func (r *NonBlockingReporter) NoWorkChan() chan struct{} {
	return r.noWorkChan
}

func (r *NonBlockingReporter) NotifyChan() chan Execution {
	return r.latencies
}

// NoWork signals there was no work to be performed.
func (r *NonBlockingReporter) NoWork() {
	select {
	case r.noWorkChan <- struct{}{}:
	default:
		// Never blocking on this call.
		// TODO: Add instrumentation for if this happens.
	}
}

// Every time
func (r *NonBlockingReporter) Work(unit func() error) {
	r.start()
	start := time.Now()

	var err error
	defer func() {
		if pr := recover(); pr != nil {
			// Making sure panic doesn't break corrupt the r.inFlight gauge.
			atomic.AddInt32(&r.inFlight, -1)
			panic(pr)
		}
		r.done(time.Now().Sub(start), err)
	}()

	err = unit()
}

func (r *NonBlockingReporter) start() {
	atomic.AddInt32(&r.inFlight, 1)
}

func (r *NonBlockingReporter) done(latency time.Duration, err error) {
	// calling done in a defer to make sure we _never_ miss decrementing
	// r.inFlight in case of panic etc.
	inflight := atomic.AddInt32(&r.inFlight, -1)

	c := r.latencies

	select {
	case c <- Execution{uint(inflight), latency, err}:
	// TODO: Investigate if we can somehow introduce a sync.Pool for
	// Executions to reduce garbage collection overhead.
	default:
		// Never blocking on this call.
		// TODO: Add instrumentation for if this happens.
	}
}
