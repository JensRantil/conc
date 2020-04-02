package conc

import (
	"time"
)

// Reporter receives feedback from processes about latencies and errors.
type Reporter interface {
	NoWork()
	Work(unit func() error)
}

type Notifier interface {
	NotifyChan() chan Execution
	NoWorkChan() chan struct{}
}

type Execution struct {
	InFlight uint
	Latency  time.Duration
	Err      error
}

// Runner is an interface implemented by you. It starts a process.
type Runner interface {
	// Start is called when your application should start another processing
	// thread. The Start function must be blocking. Start must stop processing
	// when there is an element that can be read from stopper. All processing
	// in Start must report its latency, possible errors, and if it has run out
	// of work, to r.
	Start(stopper <-chan struct{}, r Reporter)
}
