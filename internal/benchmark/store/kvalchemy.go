package store

import "github.com/ankur-anand/kvalchemy/storage"

type KVAlchemy struct {
	namespace string
	engine    *storage.Engine
}

func NewKVAlchemy(dir, namespace string) (*KVAlchemy, error) {
	engine, err := storage.NewStorageEngine(dir, namespace, nil)
	return &KVAlchemy{
		namespace: namespace,
		engine:    engine,
	}, err
}

func (k *KVAlchemy) Close() error {
	return k.engine.Close()
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
	return k.engine.TotalOpsReceived()
}
