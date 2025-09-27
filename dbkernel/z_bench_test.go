package dbkernel_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
)

func mustBenchEngine(tb testing.TB, ns string) *dbkernel.Engine {
	tb.Helper()

	dir := tb.TempDir()
	conf := dbkernel.NewDefaultEngineConfig()

	conf.ArenaSize = 512 << 20
	conf.BTreeFlushInterval = 0
	conf.WalConfig.SyncInterval = 0
	conf.WalConfig.AutoCleanup = false

	e, err := dbkernel.NewStorageEngine(dir, ns, conf)
	if err != nil {
		tb.Fatalf("NewStorageEngine(%s): %v", ns, err)
	}
	tb.Cleanup(func() {
		_ = e.Close(context.Background())
	})
	return e
}

func randKV(i int) (k, v []byte) {
	k = []byte("k-" + itoa(i))

	const payload = "-payload-256b"
	const filler = "................................................................................................"

	prefix := "v-" + itoa(i) + payload
	v = []byte(prefix + filler + filler + filler)

	return
}

func itoa(x int) string {
	if x == 0 {
		return "0"
	}
	neg := false
	if x < 0 {
		neg = true
		x = -x
	}
	var buf [20]byte
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = byte('0' + x%10)
		x /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func BenchmarkEngine_PutKV(b *testing.B) {
	e := mustBenchEngine(b, "baseline_put")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k, v := randKV(i)
		if err := e.PutKV(k, v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngine_GetKV(b *testing.B) {
	e := mustBenchEngine(b, "baseline_get")

	const seedN = 2000
	keys := make([][]byte, seedN)
	for i := 0; i < seedN; i++ {
		k, v := randKV(i)
		keys[i] = k
		if err := e.PutKV(k, v); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	idx := 0
	for i := 0; i < b.N; i++ {
		if _, err := e.GetKV(keys[idx%seedN]); err != nil {
			b.Fatal(err)
		}
		idx++
	}
}

func BenchmarkReplica_ApplyRecord_KV_Isolated(b *testing.B) {
	src := mustBenchEngine(b, "bench_src_corpus")
	rep := mustBenchEngine(b, "bench_rep_iso")
	replicator := dbkernel.NewReplicaWALHandler(rep)

	type item struct {
		enc []byte
		off []byte
	}
	items := make([]item, b.N)

	for i := 0; i < b.N; i++ {
		k := []byte("k-" + strconv.Itoa(i))
		v := bytes.Repeat([]byte{'x'}, 256)

		if err := src.PutKV(k, v); err != nil {
			b.Fatal(err)
		}
	}

	// capturing enc+offset
	r, err := src.NewReader()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		enc, off, err := r.Next()
		if err != nil {
			b.Fatalf("source.Next: %v", err)
		}
		items[i] = item{enc: append([]byte(nil), enc...), off: off.Encode()}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := replicator.ApplyRecord(items[i].enc, items[i].off); err != nil {
			b.Fatal(err)
		}
	}
}
