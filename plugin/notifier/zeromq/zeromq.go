package zeromq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	zmq "github.com/pebbe/zmq4"
)

// Notification represents a change event published to ZeroMQ.
type Notification struct {
	Key       string `json:"key"`
	Namespace string `json:"namespace"`
	Operation string `json:"operation"`
	EntryType string `json:"entry_type"`
	SeqNum    uint64 `json:"seq_num"`
}

// ZeroMQNotifier implements ZeroMQ-based notification channel.
type ZeroMQNotifier struct {
	// ZERO MQ Sockets are not thread safe.
	// https://stackoverflow.com/questions/5841896/0mq-how-to-use-zeromq-in-a-threadsafe-manner
	socket    *zmq.Socket
	mu        sync.Mutex
	bindAddr  string
	namespace string

	topicKV  string
	topicLOB string
	topicRow string

	closed     atomic.Bool
	sentCount  atomic.Uint64
	errorCount atomic.Uint64
	seqNum     atomic.Uint64
}

// Config holds ZeroMQ notifier configuration.
type Config struct {
	BindAddress string `toml:"bind_address"`
	Namespace   string `toml:"namespace"`

	HighWaterMark int `toml:"high_water_mark"`
	LingerTime    int `toml:"linger_time"`
}

// NewZeroMQNotifier creates a new ZeroMQ notifier.
func NewZeroMQNotifier(config Config) (*ZeroMQNotifier, error) {
	if config.BindAddress == "" {
		config.BindAddress = "tcp://*:5555"
	}
	if config.HighWaterMark == 0 {
		config.HighWaterMark = 10000
	}
	if config.LingerTime == 0 {
		config.LingerTime = 0
	}
	if config.Namespace == "" {
		config.Namespace = "default"
	}

	// PUB socket
	socket, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZeroMQ socket: %w", err)
	}

	// socket options
	if err := socket.SetSndhwm(config.HighWaterMark); err != nil {
		socket.Close()
		return nil, fmt.Errorf("failed to set high water mark: %w", err)
	}

	if err := socket.SetLinger(time.Duration(config.LingerTime)); err != nil {
		socket.Close()
		return nil, fmt.Errorf("failed to set linger time: %w", err)
	}

	if err := socket.Bind(config.BindAddress); err != nil {
		socket.Close()
		return nil, fmt.Errorf("failed to bind ZeroMQ socket to %s: %w", config.BindAddress, err)
	}

	notifier := &ZeroMQNotifier{
		socket:    socket,
		bindAddr:  config.BindAddress,
		namespace: config.Namespace,
		topicKV:   config.Namespace + ".kv",
		topicLOB:  config.Namespace + ".lob",
		topicRow:  config.Namespace + ".row",
	}

	slog.Info("[notifier.zeromq] ZeroMQ notifier started",
		slog.String("bind_address", config.BindAddress),
		slog.String("namespace", config.Namespace),
		slog.String("topic_kv", notifier.topicKV),
		slog.String("topic_lob", notifier.topicLOB),
		slog.String("topic_row", notifier.topicRow))

	return notifier, nil
}

// Notify sends a notification to all subscribers.
func (z *ZeroMQNotifier) Notify(ctx context.Context, key []byte, op logrecord.LogOperationType, entryType logrecord.LogEntryType) {
	if z.closed.Load() {
		return
	}

	currentSeq := z.seqNum.Add(1)

	var topic string
	switch entryType {
	case logrecord.LogEntryTypeKV:
		topic = z.topicKV
	case logrecord.LogEntryTypeChunked:
		topic = z.topicLOB
	case logrecord.LogEntryTypeRow:
		topic = z.topicRow
	default:
		topic = z.topicKV
	}

	notification := Notification{
		Key:       string(key),
		Namespace: z.namespace,
		Operation: operationTypeToString(op),
		EntryType: entryTypeToString(entryType),
		SeqNum:    currentSeq,
	}

	payload, err := json.Marshal(notification)
	if err != nil {
		z.errorCount.Add(1)
		slog.Error("[notifier.zeromq] Failed to serialize notification",
			"error", err,
			"key", string(key))
		return
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	if z.closed.Load() {
		return
	}

	// multipart message: [topic, payload]
	_, err = z.socket.SendMessage(topic, payload)
	if err != nil {
		z.errorCount.Add(1)
		slog.Error("[notifier.zeromq] Failed to send notification",
			"error", err,
			"key", string(key),
			"topic", topic)
		return
	}

	z.sentCount.Add(1)
}

// Close gracefully shuts down the notifier.
func (z *ZeroMQNotifier) Close() error {
	if !z.closed.CompareAndSwap(false, true) {
		return nil
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	slog.Info("[notifier.zeromq] Shutting down ZeroMQ notifier",
		slog.Uint64("total_sent", z.sentCount.Load()),
		slog.Uint64("total_errors", z.errorCount.Load()))

	if z.socket != nil {
		return z.socket.Close()
	}

	return nil
}

func (z *ZeroMQNotifier) Stats() (sent uint64, errors uint64) {
	return z.sentCount.Load(), z.errorCount.Load()
}

func operationTypeToString(op logrecord.LogOperationType) string {
	switch op {
	case logrecord.LogOperationTypeInsert:
		return "put"
	case logrecord.LogOperationTypeDelete:
		return "delete"
	case logrecord.LogOperationTypeDeleteRowByKey:
		return "delete_row"
	case logrecord.LogOperationTypeTxnMarker:
		return "txn_marker"
	case logrecord.LogOperationTypeNoOperation:
		return "noop"
	default:
		return "unknown"
	}
}

func entryTypeToString(et logrecord.LogEntryType) string {
	switch et {
	case logrecord.LogEntryTypeKV:
		return "kv"
	case logrecord.LogEntryTypeChunked:
		return "lob"
	case logrecord.LogEntryTypeRow:
		return "row"
	default:
		return "unknown"
	}
}
