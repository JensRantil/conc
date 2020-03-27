package conc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Reporter receives feedback from processes about latencies and errors.
type Reporter interface {
	NoWork()
	Work(unit func() error)
}

type Notifier interface {
	NotifyChan() chan ControllerReport
	ClearPendingNotifications()
	NoWorkChan() chan struct{}
}

type NonBlockingReporter struct {
	chanSize   int
	latencies  chan ControllerReport
	mLatencies sync.RWMutex

	noWorkChan chan struct{}
	inFlight   int32
}

func NewNonBlockingReporter(chanSize int) *NonBlockingReporter {
	return &NonBlockingReporter{
		chanSize:   chanSize,
		latencies:  make(chan ControllerReport, chanSize),
		noWorkChan: make(chan struct{}),
	}
}

func (r *NonBlockingReporter) NoWorkChan() chan struct{} {
	return r.noWorkChan
}

func (r *NonBlockingReporter) NotifyChan() chan ControllerReport {
	r.mLatencies.RLock()
	defer r.mLatencies.RUnlock()
	return r.latencies
}

func (r *NonBlockingReporter) ClearPendingNotifications() {
	r.mLatencies.Lock()
	defer r.mLatencies.Unlock()
	r.latencies = make(chan ControllerReport, r.chanSize)
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

	r.mLatencies.RLock()
	c := r.latencies
	r.mLatencies.RUnlock()

	select {
	case c <- ControllerReport{uint(inflight), latency, err}:
	// TODO: Investigate if we can somehow introduce a sync.Pool for
	// ControllerReports to reduce garbage collection overhead.
	default:
		// Never blocking on this call.
		// TODO: Add instrumentation for if this happens.
	}
}

// Runner is an interface implemented by you. It starts a process.
type Runner interface {
	// Start is called when your application should start another processing
	// thread. The Start function must be blocking. Start must stop processing
	// when stopper has been closed. All processing in Start must report its
	// latency, possible errors, and if it has run out of work, to r.
	//
	// TODO: Should stopper be a context.Context instead?
	Start(stopper <-chan struct{}, r Reporter)
}

// OrchestratorMetrics is called for different events in the orchestrator.
type OrchestratorMetrics interface {
	Incr(n uint)
	Decr(n uint)
	Restart()
}

// nilMetric is the default OrchestratorMetrics if none other is set.
type nilMetric struct{}

func (n *nilMetric) Incr(x uint) {}
func (n *nilMetric) Decr(x uint) {}
func (n *nilMetric) Restart()    {}

// Orchestrator keeps track of current running processes. It starts and stops them.
type Orchestrator struct {
	metrics OrchestratorMetrics

	run Runner
	rep Reporter

	stopper chan struct{}

	// number of processes currently running. Must be modified while taking
	// actualNL.
	actualN  uint
	actualNL *sync.Cond

	wantedN uint
}

type OrchestratorOpts func(*Orchestrator)

func WithMetrics(metrics OrchestratorMetrics) OrchestratorOpts {
	return func(o *Orchestrator) {
		o.metrics = metrics
	}
}

// NewOrchestrator creates an Orchestrator. The orchestrator starts with
// WantedN set to zero. Call Stop(...) to properly clean up after usage.
func NewOrchestrator(r Runner, re Reporter, opts ...OrchestratorOpts) *Orchestrator {
	res := &Orchestrator{
		&nilMetric{},
		r,
		re,
		make(chan struct{}),
		0,
		sync.NewCond(&sync.Mutex{}),
		0,
	}
	for _, o := range opts {
		o(res)
	}
	return res
}

// ActualN returns the number of processes currently running.
func (o *Orchestrator) ActualN() uint {
	o.actualNL.L.Lock()
	defer o.actualNL.L.Unlock()
	return o.actualN
}

// WantedN returns the number of processes we want running.
func (o *Orchestrator) WantedN() uint {
	return uint(o.wantedN)
}

// Incr increases the number of running processes. To wait for them to have
// shut down, call SettleDown().
func (o *Orchestrator) Incr(n uint) {
	o.wantedN += n

	o.actualNL.L.Lock()
	o.actualN += n
	o.actualNL.L.Unlock()
	o.actualNL.Broadcast()

	var i uint
	for i = 0; i < n; i++ {
		go o.runProcess()
	}
}

func (o *Orchestrator) runProcess() {
	o.run.Start(o.stopper, o.rep)

	o.actualNL.L.Lock()
	o.actualN--
	o.actualNL.L.Unlock()
	o.actualNL.Broadcast()
}

// Decr reduces the number of running processes. They will be closed async.
// To wait for them to have shut down, call SettleDown().
func (o *Orchestrator) Decr(n uint) {
	o.wantedN -= n
	if o.wantedN < 1 {
		// Can't have zero of negative number of processes.
		o.wantedN = 0
	}

	go func() {
		var i uint
		for i = 0; i < n; i++ {
			o.stopper <- struct{}{}
		}
	}()
}

// Settle waits for WantedN to be the same as ActualN.
func (o *Orchestrator) SettleDown(ctx context.Context) {
	// Consider using https://github.com/JensRantil/go-sync. Would likely simplify code.
	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-localCtx.Done()
		if ctx.Err() != nil {
			o.actualNL.Broadcast()
		}
	}()

	o.actualNL.L.Lock()
	defer o.actualNL.L.Unlock()
	for o.actualN != o.wantedN {
		if ctx.Err() != nil {
			return
		}
		o.actualNL.Wait()
	}
}

// TODO: Set a better name. Execution?
type ControllerReport struct {
	InFlight uint
	Latency  time.Duration
	Err      error
}