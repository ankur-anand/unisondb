module github.com/ankur-anand/unisondb

go 1.24.0

require (
	github.com/PowerDNS/lmdb-go v1.9.3
	github.com/anishathalye/porcupine v1.0.3
	github.com/ankur-anand/unisondb/plugin/notifier/zeromq v0.0.0-00010101000000-000000000000
	github.com/bits-and-blooms/bloom/v3 v3.7.1
	github.com/brianvoe/gofakeit/v7 v7.14.0
	github.com/cenkalti/backoff/v5 v5.0.3
	github.com/common-nighthawk/go-figure v0.0.0-20210622060536-734e95fb86be
	github.com/dgraph-io/badger/v4 v4.6.0
	github.com/dustin/go-humanize v1.0.1
	github.com/edsrzf/mmap-go v1.2.0
	github.com/fatih/color v1.18.0
	github.com/gofrs/flock v0.13.0
	github.com/google/flatbuffers v25.12.19+incompatible
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-metrics v0.5.4
	github.com/hashicorp/memberlist v0.5.4
	github.com/hashicorp/raft v1.7.3
	github.com/hashicorp/raft-boltdb/v2 v2.3.1
	github.com/hashicorp/serf v0.10.2
	github.com/hashicorp/yamux v0.1.2
	github.com/openhistogram/circonusllhist v0.4.1
	github.com/pelletier/go-toml/v2 v2.2.4
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.67.5
	github.com/segmentio/ksuid v1.0.4
	github.com/shirou/gopsutil/v4 v4.25.12
	github.com/stretchr/testify v1.11.1
	github.com/uber-go/tally/v4 v4.1.17
	github.com/urfave/cli/v2 v2.27.7
	go.etcd.io/bbolt v1.4.3
	go.opentelemetry.io/otel/trace v1.39.0
	golang.org/x/sync v0.19.0
	golang.org/x/time v0.14.0
	google.golang.org/grpc v1.78.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.2 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto/v2 v2.1.0 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/miekg/dns v1.1.68 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pebbe/zmq4 v1.4.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/mod v0.30.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	golang.org/x/tools v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251029180050-ab9386a59fda // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/ankur-anand/unisondb/plugin/notifier/zeromq => ./plugin/notifier/zeromq

//tool (
//	github.com/ajstarks/svgo/structlayout-svg
//	google.golang.org/grpc/cmd/protoc-gen-go-grpc
//	google.golang.org/protobuf/cmd/protoc-gen-go
//	honnef.co/go/tools/cmd/staticcheck
//	honnef.co/go/tools/cmd/structlayout
//	honnef.co/go/tools/cmd/structlayout-optimize
//	honnef.co/go/tools/cmd/structlayout-pretty
//)
