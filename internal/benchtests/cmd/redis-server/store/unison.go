package store

import (
	"context"
	"log"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/tests/util"
)

type KVAlchemy struct {
	namespace string
	engine    *dbkernel.Engine
}

func NewUnisonDB(ctx context.Context, dir, namespace string, relayerCount int) (*KVAlchemy, error) {
	cfg := dbkernel.NewDefaultEngineConfig()
	cfg.WriteNotifyCoalescing.Enabled = true
	cfg.WriteNotifyCoalescing.Duration = 50 * time.Millisecond
	cfg.WalConfig.SyncInterval = 1 * time.Second
	cfg.BTreeFlushInterval = 10 * time.Second
	cfg.DisableEntryTypeCheck = true
	engine, err := dbkernel.NewStorageEngine(dir, namespace, cfg)
	if relayerCount > 0 {
		go func() {
			startTime := time.Now()
			hist, err := util.StartNLocalRelayer(ctx, engine, relayerCount, 1*time.Minute)
			if err != nil {
				log.Fatal(err)
			}
			<-ctx.Done()
			util.ReportReplicationStats(hist, engine.Namespace(), startTime)

		}()
	}
	return &KVAlchemy{
		namespace: namespace,
		engine:    engine,
	}, err
}

func (k *KVAlchemy) Close() error {
	return k.engine.Close(context.Background())
}

func (k *KVAlchemy) Set(key, value []byte) error {
	return k.engine.PutKV(key, value)
}

func (k *KVAlchemy) Get(key []byte) ([]byte, error) {
	return k.engine.GetKV(key)
}

func (k *KVAlchemy) TotalOpsCount() uint64 {
	return k.engine.OpsReceivedCount()
}

func (k *KVAlchemy) SealedMemTableCount() int {
	return k.engine.SealedMemTableCount()
}
