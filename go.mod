module github.com/ankur-anand/unisondb

go 1.25.2

require (
	github.com/PowerDNS/lmdb-go v1.9.3
	github.com/anishathalye/porcupine v1.0.3
	github.com/ankur-anand/isledb v0.2.0
	github.com/ankur-anand/unisondb/plugin/notifier/zeromq v0.0.0-00010101000000-000000000000
	github.com/bits-and-blooms/bloom/v3 v3.7.1
	github.com/brianvoe/gofakeit/v7 v7.14.0
	github.com/cenkalti/backoff/v5 v5.0.3
	github.com/common-nighthawk/go-figure v0.0.0-20210622060536-734e95fb86be
	github.com/dgraph-io/badger/v4 v4.9.1
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
	go.opentelemetry.io/otel/trace v1.42.0
	golang.org/x/sync v0.20.0
	golang.org/x/time v0.15.0
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.11
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.19.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.6.0 // indirect
	cloud.google.com/go/monitoring v1.24.3 // indirect
	cloud.google.com/go/storage v1.61.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.4 // indirect
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.31.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.55.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.55.0 // indirect
	github.com/RaduBerinde/axisds v0.1.0 // indirect
	github.com/RaduBerinde/btreemap v0.0.0-20260105202824-d3184786f603 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.5 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.97.3 // indirect
	github.com/aws/smithy-go v1.24.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.2 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/cockroachdb/crlib v0.0.0-20251122031428-fe658a2dbda1 // indirect
	github.com/cockroachdb/errors v1.12.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/pebble/v2 v2.1.4 // indirect
	github.com/cockroachdb/redact v1.1.8 // indirect
	github.com/cockroachdb/swiss v0.0.0-20251224182025-b0f6560f979b // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20250429170803-42689b6311bb // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto/v2 v2.4.0 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/getsentry/sentry-go v0.44.1 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.14 // indirect
	github.com/googleapis/gax-go/v2 v2.20.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/miekg/dns v1.1.68 // indirect
	github.com/minio/minlz v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pebbe/zmq4 v1.4.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.42.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.42.0 // indirect
	go.opentelemetry.io/otel/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.42.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	gocloud.dev v0.45.0 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/api v0.273.0 // indirect
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260319201613-d00831a3d3e7 // indirect
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
