package store

import (
	"context"

	"github.com/ankur-anand/kvalchemy/dbengine"
)

type KVAlchemy struct {
	namespace string
	engine    *dbengine.Engine
}

func NewKVAlchemy(dir, namespace string) (*KVAlchemy, error) {
	cfg := dbengine.NewDefaultEngineConfig()
	cfg.ValueThreshold = 100 * 1024
	engine, err := dbengine.NewStorageEngine(dir, namespace, cfg)
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

func (k *KVAlchemy) Delete(key []byte) error {
	return k.engine.Delete(key)
}
func (k *KVAlchemy) Get(key []byte) ([]byte, error) {
	return k.engine.Get(key)
}

func (k *KVAlchemy) TotalOpsCount() uint64 {
	return k.engine.OpsReceivedCount()
}
