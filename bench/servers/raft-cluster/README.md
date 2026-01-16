Raft Bench (3-node, local)

Overview
- Spins up 3 local UnisonDB nodes with Serf membership + Raft enabled.
- Runs a write-only HTTP benchmark against the leader.
- Default: 32 workers, 30s warmup, 2m measurement, sizes 256/512/1024 bytes.

Setup
1. From repo root, clear previous data:
   `rm -rf bench/servers/raft-cluster/raftdata/node1 bench/servers/raft-cluster/raftdata/node2 bench/servers/raft-cluster/raftdata/node3`
2. Start three nodes:
   - Script: `./bench/servers/raft-cluster/run_nodes.sh`
    - Manual:
     `go run ./cmd/unisondb server -c bench/servers/raft-cluster/node1.toml`
     `go run ./cmd/unisondb server -c bench/servers/raft-cluster/node2.toml`
     `go run ./cmd/unisondb server -c bench/servers/raft-cluster/node3.toml`
3. Wait for the leader to stabilize (node0 should be leader by default).
   - If leadership changes, point the benchmark at the leader's HTTP port.

Benchmark
- Run against the leader's HTTP API:
  `go run ./bench/client --base-url http://127.0.0.1:4001/api/v1 --namespace default --workers 32 --warmup 30s --duration 2m --sizes 256,512,1024`
  - Add `--progress 10s` to log progress; set `--progress 0` to disable.

Notes
- Writes must target the leader; followers return errors for write ops.
- All data is stored under `bench/servers/raft-cluster/raftdata/node{1,2,3}`.
- `run_nodes.sh` removes data dirs on exit by default. Set `CLEAN_DATA=0` to keep data, or `CLEAN_LOGS=1` to delete logs too.

