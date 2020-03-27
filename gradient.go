package conc

import (
	"context"
	"log"
	"math"
	"sync"
	"time"
)

const DefaultMaxConcurrency = 20

type GradientOpts func(*GradientController)

func WithRttTolerance(rttt float32) GradientOpts {
	return func(g *GradientController) {
		g.rttTolerance = rttt
	}
}

func WithSmoothing(s float32) GradientOpts {
	return func(g *GradientController) {
		g.smoothing = s
	}
}

func WithQueueSize(q func(uint) uint) GradientOpts {
	return func(g *GradientController) {
		g.queueSize = q
	}
}

func WithProbeInterval(i uint) GradientOpts {
	return func(g *GradientController) {
		g.probeInterval = i
	}
}

// WithBackoffRatio sets
func WithBackoffRatio(b float32) GradientOpts {
	return func(g *GradientController) {
		g.backoffRatio = b
	}
}

func WithInitialLimit(b uint) GradientOpts {
	return func(g *GradientController) {
		g.initial = b
	}
}

func WithMinLimit(b uint) GradientOpts {
	return func(g *GradientController) {
		g.min = b
	}
}

func WithMaxLimit(b uint) GradientOpts {
	return func(g *GradientController) {
		g.max = b
	}
}

// NewGradientController creates a new GradientController. Call the Start()
// method to make it run. Once done, make sure to call Stop() to clear upp
// resources. After stopped, the controller can be started again if you want
// to.
func NewGradientController(n Notifier, orch *Orchestrator, opts ...GradientOpts) *GradientController {
	c := &GradientController{
		notif:         n,
		orch:          orch,
		initial:       1,
		min:           1,
		max:           DefaultMaxConcurrency,
		rttTolerance:  2.0,
		smoothing:     0.2,
		queueSize:     sqrt,
		probeInterval: 1000,
		backoffRatio:  0.9,
	}
	for _, o := range opts {
		o(c)
	}

	// Check argument invariants.
	if c.min > c.max {
		panic("min can't be greater than max.")
	}
	if c.initial < c.min {
		panic("initial can't be less than min.")
	}
	if c.initial > c.max {
		panic("initial can't be greater than max.")
	}

	return c
}

// GradientController delegates concurrency limits to SimplifiedController,
// adding basic limits such as minimum and maximum concurrency.
type GradientController struct {
	notif Notifier
	orch  *Orchestrator

	initial uint
	min     uint
	max     uint

	// Inspired by [1].
	//
	// [1] https://github.com/Netflix/concurrency-limits/blob/18692b09e55a0574bea94d92e95a03c3e89012d2/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java
	rttTolerance  float32
	smoothing     float32
	queueSize     func(uint) uint
	probeInterval uint
	backoffRatio  float32

	// Variables that are modified by the control loop.
	//
	// TODO: Refactor the update method out into a separate type to avoid these variables bloating this type.
	resetRttCounter uint
	resetNoLoadRtt  bool
	noLoadRtt       time.Duration

	// The following variables are instantiated at start.
	quitChan chan struct{}
	wg       sync.WaitGroup
}

func (c *GradientController) Start() {
	c.quitChan = make(chan struct{})
	c.wg = sync.WaitGroup{}
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		c.orch.Incr(c.initial)
		c.resetRttCounter = c.nextResetCounter()
		c.resetNoLoadRtt = true

		for {
			select {
			case <-c.quitChan:
				return
			case r := <-c.notif.NotifyChan():
				c.adjust(c.update(r), true)
			case <-c.notif.NoWorkChan():
				// TODO: Can this be done in a better way? Are we shutting down
				// worker goroutines too fast or too slow?
				if newLimit := c.orch.WantedN() - 1; newLimit >= c.min {
					c.adjust(newLimit, false)
				}
			}
		}
	}()
}

func (c *GradientController) update(r ControllerReport) uint {
	// This function is hugely inspired by [1].
	//
	// [1] https://github.com/Netflix/concurrency-limits/blob/18692b09e55a0574bea94d92e95a03c3e89012d2/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java#L259

	currLimit := c.orch.WantedN()
	queueSize := c.queueSize(currLimit)

	c.resetRttCounter--
	if c.resetRttCounter <= 0 {
		c.resetRttCounter = c.nextResetCounter()
		c.resetNoLoadRtt = true
		return queueSize
	}

	if c.resetNoLoadRtt || c.noLoadRtt > r.Latency {
		c.noLoadRtt = r.Latency
		c.resetNoLoadRtt = false
	}

	// TODO: Remove this line and make this configurable to be logged or not.
	log.Println("Reported latency:", r.Latency, "NoLoadRtt:", c.noLoadRtt)

	gradient := maxf(0.5, minf(1.0, c.rttTolerance*float32(c.noLoadRtt)/float32(r.Latency)))

	fcurrLimit := float32(currLimit)
	var newLimit float32
	if r.Err != nil {
		newLimit = fcurrLimit * c.backoffRatio
	} else {
		newLimit = fcurrLimit*gradient + float32(queueSize)
	}

	if newLimit < fcurrLimit {
		newLimit = (1-c.smoothing)*fcurrLimit + c.smoothing*newLimit
	}

	return max(queueSize, uint(newLimit))
}

func (c *GradientController) nextResetCounter() uint {
	// TODO: Add randomness here. See https://github.com/Netflix/concurrency-limits/blob/18692b09e55a0574bea94d92e95a03c3e89012d2/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/GradientLimit.java#L255
	return c.probeInterval
}

func (c *GradientController) adjust(newLimit uint, settle bool) {
	newLimit = max(newLimit, c.min)
	newLimit = min(newLimit, c.max)

	// TODO: Remove this line and make this configurable to be logged or not.
	log.Println("New limit:", newLimit)

	// TODO: Move this if case into a new function in the orchestrator and make
	// Incr/Decr unexported
	currLimit := c.orch.WantedN()
	if newLimit > currLimit {
		c.orch.Incr(newLimit - currLimit)
	} else if currLimit > newLimit {
		c.orch.Decr(currLimit - newLimit)
	} else /* currLimit==newLimit */ {
		return
	}
	if settle {
		// TODO: Support for injecting a custom context for the
		// GradientController. Stop() function should cancel it.
		c.orch.SettleDown(context.TODO())
	}

	// Make sure the latencies we'll be collecting are only since the
	// new changes have taken effect.
	c.notif.ClearPendingNotifications()
}

func min(a, b uint) uint {
	if a < b {
		return a
	} else {
		return b
	}
}
func max(a, b uint) uint {
	if a > b {
		return a
	} else {
		return b
	}
}

func minf(a, b float32) float32 {
	if a < b {
		return a
	} else {
		return b
	}
}
func maxf(a, b float32) float32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func (c *GradientController) Stop(ctx context.Context) {
	// First close the orchestrator so that it doesn't mess with this thread when we are shutting down all goroutines...
	close(c.quitChan)
	c.wg.Wait()

	// ...then we ask the orchestrator to shut down all worker threads.
	c.orch.Decr(c.orch.WantedN())
	c.orch.SettleDown(ctx)
}

func sqrt(x uint) uint {
	// TODO: Make this faster by having a lookup table for common x, similarly
	// to what the concurrency-limits library does.
	return uint(math.Round(math.Sqrt(float64(x))))
}
