## Tools used:

Check go.mod for tool declaration.

## How to run Benchmark and compare.

Benchmarking Variance?: Run on Fixed CPU Cores
```shell
GOMAXPROCS=4 go test -bench=. -benchmem
```

### profile memory and cpu usage:

```shell
go test -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof
go tool pprof -http=:8080 mem.prof
go tool pprof -http=:8080 cpu.prof
```
### If version is being updated, Run Benchmark against version and compare.

```shell
go1.20 test -bench=. -benchmem > go1.20.txt
go1.21 test -bench=. -benchmem > go1.21.txt
benchstat go1.20.txt go1.21.txt
```

### Compare GC activity between runs in benchmark.

```shell
GODEBUG=gctrace=1 go test -bench=. -benchmem 2> gc_log.txt
```

If allocations increased, check:
•	Did garbage collection frequency increase?
•	Are slices/maps growing unexpectedly?

### plotting GC
