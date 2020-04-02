package main

import (
	"context"
	"flag"
	"sync/atomic"
	"time"

	"github.com/JensRantil/conc"
	"gonum.org/v1/gonum/stat/distuv"
)

func main() {
	reporterDepth := flag.Int("reporter-depth", 0, "The size of the channel used for reporting latencies. You might want to increase this if you expect bursty traffic and want to make sure your controller can keep up with the feedback.")
	maxConcurrency := flag.Uint("max-concurrency", 20, "The maximum concurrency of downstream serving layer.")
	minLimit := flag.Uint("min-limit", 1, "The minimum limit of workers.")
	maxLimit := flag.Uint("max-limit", 100, "The maximum limit of workers.")
	initialLimit := flag.Uint("initial-limit", 1, "The initial limit of workers.")
	probeInterval := flag.Uint("probe-interval", 1000, "How often we should probe for the smallest ")
	rttTolerance := flag.Float64("rtt-tolerance", 2.0, "How sensitive the controller should be.")
	smoothing := flag.Float64("smoothing", 0.2, "How smooth the changes are to be.")
	flag.Parse()

	r := conc.NewNonBlockingReporter(*reporterDepth)
	reqduration := make(chan time.Duration)

	// Pareto based on https://stats.stackexchange.com/a/179059.
	requestdist := distuv.Pareto{
		Xm:    200,
		Alpha: 15,
	}

	orch := conc.NewOrchestrator(&testRunner{
		MaxConcurrency: *maxConcurrency,
		ReqDurations:   reqduration,
		ReqDist:        requestdist,
	}, r)
	controller := conc.NewGradientController(
		r,
		orch,
		conc.WithMinLimit(*minLimit),
		conc.WithMaxLimit(*maxLimit),
		conc.WithInitialLimit(*initialLimit),
		conc.WithProbeInterval(*probeInterval),
		conc.WithRttTolerance(float64(*rttTolerance)),
		conc.WithSmoothing(float64(*smoothing)),
	)

	controller.Start()
	// TODO: Have the workers process from a channel.
	// TODO: Log adjustments to concurrency.

	for i := 0; i < 5000; i++ {
		// The time it takes to process a request is a result of actually
		// handling the request, plus the waiting time (queue) until the
		// request can be handled. Now, let's assume that the time handling of
		// the request is independent of number of pending requests.
		reqduration <- time.Duration(requestdist.Rand() * float64(time.Millisecond))
	}

	controller.Stop(context.TODO())
}

type testRunner struct {
	MaxConcurrency uint
	RunningTasks   int32
	ReqDist        distuv.Pareto

	ReqDurations <-chan time.Duration
}

func (t *testRunner) Start(stopper <-chan struct{}, r conc.Reporter) {
	for {
		select {
		case reqduration := <-t.ReqDurations:
			r.Work(func() error {
				nrunning := atomic.AddInt32(&t.RunningTasks, 1)
				if uint(nrunning) > t.MaxConcurrency {
					// Tasks have queued up.
					waiting := uint(nrunning) - t.MaxConcurrency
					queuetime := time.Duration(float64(waiting) * t.ReqDist.Rand() * float64(time.Millisecond) / float64(t.MaxConcurrency))
					time.Sleep(queuetime)
				}
				time.Sleep(reqduration)
				atomic.AddInt32(&t.RunningTasks, -1)
				return nil
			})
		case <-stopper:
			return
		}

	}
}
