## Redis Server Build Redis Compatible Server for testing with Redis-Bench.

## running benchmark
```sh
redis-benchmark -d 1000 -t set  -r 10000 -n 10000 -p 6380 -q
```
