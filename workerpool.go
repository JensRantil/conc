package conc

import (
	"context"
	"sync"
)

// WorkerPoolMetrics is called for different events in the orchestrator.
type WorkerPoolMetrics interface {
	Incr(n uint)
	Decr(n uint)
	Restart()
}

// nilMetric is the default WorkerPoolMetrics if none other is set.
type nilMetric struct{}

func (n *nilMetric) Incr(x uint) {}
func (n *nilMetric) Decr(x uint) {}
func (n *nilMetric) Restart()    {}

// WorkerPool keeps track of current running processes. It starts and stops them.
type WorkerPool struct {
	metrics WorkerPoolMetrics

	run Runner
	rep Reporter

	stopper chan struct{}

	// number of processes currently running. Must be modified while taking
	// actualNL.
	actualN  uint
	actualNL *sync.Cond

	wantedN uint
}

type WorkerPoolOpts func(*WorkerPool)

func WithMetrics(metrics WorkerPoolMetrics) WorkerPoolOpts {
	return func(o *WorkerPool) {
		o.metrics = metrics
	}
}

// NewWorkerPool creates an WorkerPool. The orchestrator starts with
// WantedN set to zero. Call Stop(...) to properly clean up after usage.
func NewWorkerPool(r Runner, re Reporter, opts ...WorkerPoolOpts) *WorkerPool {
	res := &WorkerPool{
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
func (o *WorkerPool) ActualN() uint {
	o.actualNL.L.Lock()
	defer o.actualNL.L.Unlock()
	return o.actualN
}

// WantedN returns the number of processes we want running.
func (o *WorkerPool) WantedN() uint {
	return uint(o.wantedN)
}

// Incr increases the number of running processes. To wait for them to have
// shut down, call SettleDown().
func (o *WorkerPool) Incr(n uint) {
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

func (o *WorkerPool) runProcess() {
	o.run.Start(o.stopper, o.rep)

	o.actualNL.L.Lock()
	o.actualN--
	o.actualNL.L.Unlock()
	o.actualNL.Broadcast()
}

// Decr reduces the number of running processes. They will be closed async.
// To wait for them to have shut down, call SettleDown().
func (o *WorkerPool) Decr(n uint) {
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
func (o *WorkerPool) SettleDown(ctx context.Context) {
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
