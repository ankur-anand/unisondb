package store

import (
	"context"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
)

type KVAlchemy struct {
	namespace string
	engine    *dbkernel.Engine
}

func NewUnisonDB(dir, namespace string) (*KVAlchemy, error) {
	cfg := dbkernel.NewDefaultEngineConfig()
	cfg.WalConfig.SyncInterval = 1 * time.Second
	cfg.BTreeFlushInterval = 10 * time.Second
	engine, err := dbkernel.NewStorageEngine(dir, namespace, cfg)
	return &KVAlchemy{
		namespace: namespace,
		engine:    engine,
	}, err
}

func (k *KVAlchemy) Close() error {
	return k.engine.Close(context.Background())
}

func (k *KVAlchemy) Set(key, value []byte) error {
	return k.engine.Put(key, value)
}

func (k *KVAlchemy) Get(key []byte) ([]byte, error) {
	return k.engine.Get(key)
}

func (k *KVAlchemy) TotalOpsCount() uint64 {
	return k.engine.OpsReceivedCount()
}
