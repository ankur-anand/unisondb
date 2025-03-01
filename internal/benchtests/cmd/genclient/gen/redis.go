package gen

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/huandu/skiplist"
	"github.com/openhistogram/circonusllhist"
	"github.com/redis/go-redis/v9"
)

// RedisGen Getter and Setter
type RedisGen struct {
	ctx        context.Context
	redis      *redis.Client
	mu         sync.RWMutex
	db         map[string]string
	sk         *skiplist.SkipList
	dataSize   uint
	WriteIndex atomic.Uint64
	ReadIndex  atomic.Uint64
	GetLatency *circonusllhist.Histogram
	SetLatency *circonusllhist.Histogram
}

func NewRedisGen(ctx context.Context, client *redis.Client, dataSize uint) *RedisGen {
	return &RedisGen{
		ctx:        ctx,
		redis:      client,
		db:         make(map[string]string),
		sk:         skiplist.New(skiplist.Uint64),
		dataSize:   dataSize,
		SetLatency: circonusllhist.New(),
		GetLatency: circonusllhist.New(),
	}
}

func (r *RedisGen) Set() {
	// generate a random key and value.
	r.mu.Lock()
	defer r.mu.Unlock()
	key := gofakeit.LetterN(50)
	value := gofakeit.LetterN(r.dataSize)

	r.db[key] = value
	start := time.Now()
	err := r.redis.Set(r.ctx, key, value, time.Duration(0)).Err()
	r.SetLatency.RecordDuration(time.Since(start))
	if err != nil {
		slog.Error("redis set err:", "err", err)
	}
	r.sk.Set(r.WriteIndex.Add(1), key)
}

func (r *RedisGen) Get() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	val, ok := r.sk.GetValue(r.ReadIndex.Add(1))
	if !ok {
		return
	}
	key := val.(string)
	start := time.Now()
	cmd := r.redis.Get(r.ctx, key)
	r.GetLatency.RecordDuration(time.Since(start))
	if cmd.Err() != nil {
		slog.Error("redis get err:", "err", cmd.Err())
	}
	if r.db[key] != cmd.Val() {
		slog.Error("redis get val err", "key", key, "val", r.db[key])
	}
}
