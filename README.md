conc
====
[![GoDoc](https://godoc.org/github.com/JensRantil/conc?status.svg)](https://godoc.org/github.com/JensRantil/conc)

**Status:** Alpha. Please file issues for all things high and low (including of
naming of types and functions).

A library that dynamically adjusts concurrency of a worker pool based on
observed latency changes. Thereby constantly trying to achieve optimal
throughput without incurring latency costs.

`conc` is a concurrency library inspired by Netflix's
[`concurrency-limits`](https://github.com/Netflix/concurrency-limits) library.
Please read [Performance Under
Load](https://medium.com/@NetflixTechBlog/performance-under-load-3e6fa9a60581)
to understand more details.

Compared to Netflix's library, which is thread pool-oriented, this library is
worker pool-oriented. The benefit is that it more easily can be used _both_ for

 * thread pool scenarios (by submitting work/functions to a shared channel); and
 * worker pool scenarios such as processing things from a worker queue.

There's a simulator that allows you to test out the library. To play around
with it:
```sh
$ git clone github.com/JensRantil/conc
$ cd conc/simulator
$ go run . -help
```
