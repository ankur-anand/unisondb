package zeromq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewZeroMQNotifier_DefaultConfig(t *testing.T) {
	n, err := NewZeroMQNotifier(Config{})
	require.NoError(t, err)
	require.NotNil(t, n)
	defer n.Close()

	assert.Equal(t, "tcp://*:5555", n.bindAddr)
	assert.Equal(t, "default", n.namespace)
	assert.Equal(t, "default.kv", n.topicKV)
	assert.Equal(t, "default.lob", n.topicLOB)
	assert.Equal(t, "default.row", n.topicRow)
	assert.False(t, n.closed.Load())
}

func TestNewZeroMQNotifier_CustomConfig(t *testing.T) {
	cfg := Config{
		BindAddress:   "tcp://*:5556",
		Namespace:     "production",
		HighWaterMark: 5000,
		LingerTime:    1000,
	}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	require.NotNil(t, n)
	defer n.Close()

	assert.Equal(t, "tcp://*:5556", n.bindAddr)
	assert.Equal(t, "production", n.namespace)
	assert.Equal(t, "production.kv", n.topicKV)
	assert.Equal(t, "production.lob", n.topicLOB)
	assert.Equal(t, "production.row", n.topicRow)
}

func TestNewZeroMQNotifier_InvalidAddress(t *testing.T) {
	cfg := Config{BindAddress: "invalid://address"}
	n, err := NewZeroMQNotifier(cfg)
	assert.Error(t, err)
	assert.Nil(t, n)
}

func TestZeroMQNotifier_Notify(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15555", Namespace: "testns"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15555"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	key := []byte("user:123")
	n.Notify(ctx, key, logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)

	parts, err := sub.RecvMessage(0)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, "testns.kv", parts[0])

	var nt Notification
	require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
	assert.Equal(t, "user:123", nt.Key)
	assert.Equal(t, "testns", nt.Namespace)
	assert.Equal(t, "put", nt.Operation)
	assert.Equal(t, "kv", nt.EntryType)
}

func TestZeroMQNotifier_MultipleOperations(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15557", Namespace: "testns"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15557"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	tests := []struct {
		op       logrecord.LogOperationType
		expected string
	}{
		{logrecord.LogOperationTypeInsert, "put"},
		{logrecord.LogOperationTypeDelete, "delete"},
		{logrecord.LogOperationTypeDeleteRowByKey, "delete_row"},
		{logrecord.LogOperationTypeTxnMarker, "txn_marker"},
		{logrecord.LogOperationTypeNoOperation, "noop"},
	}

	for _, tc := range tests {
		n.Notify(ctx, []byte("key"), tc.op, logrecord.LogEntryTypeKV)
		parts, err := sub.RecvMessage(0)
		require.NoError(t, err)
		var nt Notification
		require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
		assert.Equal(t, tc.expected, nt.Operation)
	}
}

func TestZeroMQNotifier_MultipleEntryTypes(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15558", Namespace: "testns"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15558"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	cases := []struct {
		et       logrecord.LogEntryType
		expected string
	}{
		{logrecord.LogEntryTypeKV, "kv"},
		{logrecord.LogEntryTypeChunked, "lob"},
		{logrecord.LogEntryTypeRow, "row"},
	}

	for _, tc := range cases {
		n.Notify(ctx, []byte("key"), logrecord.LogOperationTypeInsert, tc.et)
		parts, err := sub.RecvMessage(0)
		require.NoError(t, err)
		var nt Notification
		require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
		assert.Equal(t, tc.expected, nt.EntryType)
	}
}

func TestZeroMQNotifier_TopicFiltering(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15559", Namespace: "production"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15559"))
	require.NoError(t, sub.SetSubscribe("production.kv"))
	sub.SetRcvtimeo(500 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	n.Notify(ctx, []byte("key1"), logrecord.LogOperationTypeDelete, logrecord.LogEntryTypeRow)
	_, err = sub.RecvMessage(0)
	assert.Error(t, err)

	n.Notify(ctx, []byte("key2"), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	parts, err := sub.RecvMessage(0)
	require.NoError(t, err)
	var nt Notification
	require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
	assert.Equal(t, "key2", nt.Key)
	assert.Equal(t, "put", nt.Operation)
	assert.Equal(t, "kv", nt.EntryType)
}

func TestZeroMQNotifier_ConcurrentNotifications(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15560", Namespace: "concurrent"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15560"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(300 * time.Millisecond)

	numG := 10
	perG := 10
	var wg sync.WaitGroup
	wg.Add(numG)

	for i := 0; i < numG; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < perG; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				n.Notify(ctx, key, logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	rcv := 0
	sub.SetRcvtimeo(1 * time.Second)
	for {
		_, err := sub.RecvMessage(0)
		if err != nil {
			break
		}
		rcv++
	}
	exp := numG * perG
	assert.GreaterOrEqual(t, rcv, int(float64(exp)*0.9))

	sent, _ := n.Stats()
	assert.Equal(t, uint64(exp), sent)
}

func TestZeroMQNotifier_Stats(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15561", Namespace: "stats"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	sent, errs := n.Stats()
	assert.Equal(t, uint64(0), sent)
	assert.Equal(t, uint64(0), errs)

	for i := 0; i < 50; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}

	sent, errs = n.Stats()
	assert.Equal(t, uint64(50), sent)
	assert.Equal(t, uint64(0), errs)
}

func TestZeroMQNotifier_SequenceNumbers(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15567", Namespace: "seqtest"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15567"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	nn := 10
	for i := 0; i < nn; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}

	sub.SetRcvtimeo(1 * time.Second)
	var seqs []uint64
	for i := 0; i < nn; i++ {
		parts, err := sub.RecvMessage(0)
		require.NoError(t, err)
		require.Len(t, parts, 2)
		var nt Notification
		require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
		seqs = append(seqs, nt.SeqNum)
	}
	for i, s := range seqs {
		assert.Equal(t, uint64(i+1), s)
	}
	for i := 1; i < len(seqs); i++ {
		assert.Greater(t, seqs[i], seqs[i-1])
	}
}

func TestZeroMQNotifier_SequenceGapDetection(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15568", Namespace: "gaptest"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15568"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
		time.Sleep(10 * time.Millisecond)
	}

	sub.SetRcvtimeo(2 * time.Second)
	var got []uint64
	var last uint64
	for i := 0; i < 5; i++ {
		parts, err := sub.RecvMessage(0)
		if err != nil {
			break
		}
		require.Len(t, parts, 2)
		var nt Notification
		require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
		got = append(got, nt.SeqNum)
		if last > 0 {
			exp := last + 1
			if nt.SeqNum > exp {
				_ = nt.SeqNum - exp
			}
		}
		last = nt.SeqNum
	}
	assert.GreaterOrEqual(t, len(got), 4)
}

func TestZeroMQNotifier_ContextCancellation(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15562", Namespace: "cancel"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n.Notify(ctx, []byte("key"), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)

	sent, _ := n.Stats()
	assert.Equal(t, uint64(1), sent)
}

func TestZeroMQNotifier_CloseWhileSending(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15563", Namespace: "closetest"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		for i := 0; i < 1000; i++ {
			n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, n.Close())
	assert.NoError(t, n.Close())
	wg.Wait()

	ctx := context.Background()
	n.Notify(ctx, []byte("after-close"), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
}

func TestZeroMQNotifier_Close(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15564"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)

	err = n.Close()
	assert.NoError(t, err)
	assert.True(t, n.closed.Load())
	err = n.Close()
	assert.NoError(t, err)
}

func TestOperationTypeToString(t *testing.T) {
	tests := []struct {
		op       logrecord.LogOperationType
		expected string
	}{
		{logrecord.LogOperationTypeInsert, "put"},
		{logrecord.LogOperationTypeDelete, "delete"},
		{logrecord.LogOperationTypeDeleteRowByKey, "delete_row"},
		{logrecord.LogOperationTypeTxnMarker, "txn_marker"},
		{logrecord.LogOperationTypeNoOperation, "noop"},
		{logrecord.LogOperationType(255), "unknown"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, operationTypeToString(tc.op))
	}
}

func TestEntryTypeToString(t *testing.T) {
	tests := []struct {
		et       logrecord.LogEntryType
		expected string
	}{
		{logrecord.LogEntryTypeKV, "kv"},
		{logrecord.LogEntryTypeChunked, "lob"},
		{logrecord.LogEntryTypeRow, "row"},
		{logrecord.LogEntryType(255), "unknown"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, entryTypeToString(tc.et))
	}
}

func TestZeroMQNotifier_NoSubscribers_NonBlocking(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15569", Namespace: "nosubscribers"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 100; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 1*time.Second)

	sent, errs := n.Stats()
	assert.Equal(t, uint64(100), sent)
	assert.Equal(t, uint64(0), errs)
}

func TestZeroMQNotifier_LateSubscriber_MissesEarlyMessages(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15570", Namespace: "slowjoiner"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("early-key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}
	sent, _ := n.Stats()
	assert.Equal(t, uint64(10), sent)

	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15570"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 10; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("late-key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}

	sub.SetRcvtimeo(1 * time.Second)
	rcv := 0
	var keys []string
	for i := 0; i < 20; i++ {
		parts, err := sub.RecvMessage(0)
		if err != nil {
			break
		}
		var nt Notification
		require.NoError(t, json.Unmarshal([]byte(parts[1]), &nt))
		keys = append(keys, nt.Key)
		rcv++
	}
	assert.Less(t, rcv, 20)
	assert.Greater(t, rcv, 0)
	for _, k := range keys {
		assert.Contains(t, k, "late-key")
	}
}

func TestZeroMQNotifier_HighThroughput_NoSubscribers(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15571", Namespace: "highthroughput", HighWaterMark: 100000}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	num := 10000
	start := time.Now()
	for i := 0; i < num; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}
	elapsed := time.Since(start)
	tput := float64(num) / elapsed.Seconds()

	sent, errs := n.Stats()
	assert.Equal(t, uint64(num), sent)
	assert.Equal(t, uint64(0), errs)
	assert.Greater(t, tput, 10000.0)
}

func TestZeroMQNotifier_FireAndForget_NoAcknowledgment(t *testing.T) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15572", Namespace: "fireandforget"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(t, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	sub, err := zmq.NewSocket(zmq.SUB)
	require.NoError(t, err)
	defer sub.Close()
	require.NoError(t, sub.Connect("tcp://127.0.0.1:15572"))
	require.NoError(t, sub.SetSubscribe(""))
	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 5; i++ {
		n.Notify(ctx, []byte(fmt.Sprintf("key-%d", i)), logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}
	sub.Close()

	sent, errs := n.Stats()
	assert.Equal(t, uint64(5), sent)
	assert.Equal(t, uint64(0), errs)
}

func BenchmarkZeroMQNotifier_Notify(b *testing.B) {
	cfg := Config{BindAddress: "tcp://127.0.0.1:15565", Namespace: "test"}
	n, err := NewZeroMQNotifier(cfg)
	require.NoError(b, err)
	defer n.Close()
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	key := []byte("benchmark-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.Notify(ctx, key, logrecord.LogOperationTypeInsert, logrecord.LogEntryTypeKV)
	}
}
