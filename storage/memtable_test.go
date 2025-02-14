package storage

import (
	"bytes"
	"errors"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func setupMemTable(t *testing.T, capacity int64) *memTable {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := newBoltdb(dbFile)
	assert.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close db: %s", err)
		}
	})

	// Create namespace bucket in BoltDB
	err = db.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(testNamespace))
		return err
	})
	assert.NoError(t, err)

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.Open(newWALOptions(walDir))
	assert.NoError(t, err)
	t.Cleanup(func() {
		walInstance.Close()
	})

	if capacity == 0 {
		capacity = 1 * 1024 * 1024
	}
	return newMemTable(capacity, db, walInstance, testNamespace)
}

func TestMemTable_PutAndGet(t *testing.T) {

	const capacity = wal.MB
	table := newMemTable(capacity, nil, nil, "")

	// Create a test key, value, and WAL position.
	key := []byte("test-key")
	val := y.ValueStruct{Value: []byte("test-value")}
	pos := &wal.ChunkPosition{SegmentId: 1}

	// Verify that canPut returns true initially.
	if !table.canPut(key, val) {
		t.Fatalf("expected canPut to return true for key %q", key)
	}

	err := table.put(key, val, pos)
	assert.NoError(t, err, "unexpected error on put")

	gotVal := table.get(key)
	// Use reflect.DeepEqual (or a more specific comparison) to check equality.
	if bytes.Compare(gotVal.Value, val.Value) != 0 {
		t.Errorf("got value %+v, expected %+v", gotVal, val)
	}

	// Verify that the table's WAL position was updated.
	if table.lastWalPosition != pos {
		t.Errorf("expected table lastWalPosition to be %+v, got %+v", pos, table.lastWalPosition)
	}
}

func TestMemTable_CannotPut(t *testing.T) {
	const capacity = 1 * wal.KB // a very small arena capacity for testing
	table := newMemTable(capacity, nil, nil, "")

	key := []byte("key")
	// more than 1 KB
	value := gofakeit.LetterN(1100)
	val := y.ValueStruct{Value: []byte(value)}
	pos := &wal.ChunkPosition{SegmentId: 1}

	// should not panic
	err := table.put(key, val, pos)
	if !errors.Is(err, errArenaSizeWillExceed) {
		t.Fatalf("expected error %q, got %v", errArenaSizeWillExceed, err)
	}
}

func TestFlush_Success(t *testing.T) {
	mmTable := setupMemTable(t, 0)
	for i := 0; i < 150; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))

		record := walRecord{
			hlc:   uint64(i),
			key:   key,
			value: value,
			op:    wrecord.LogOperationOpInsert,
		}

		encodedRecord, err := record.fbEncode()
		assert.NoError(t, err, "failed to encode record")
		walPos, err := mmTable.wal.Write(encodedRecord)
		assert.NoError(t, err)

		// Store WAL ChunkPosition in MemTable
		mmTable.skipList.Put(y.KeyWithTs(key, 0), y.ValueStruct{
			Meta:  byte(wrecord.LogOperationOpInsert),
			Value: append([]byte{0}, walPos.Encode()...), // Storing WAL position
		})
	}

	_, err := mmTable.flush()
	assert.NoError(t, err)

	// **Verify
	for i := 0; i < 150; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		val, err := mmTable.db.Get(testNamespace, key)
		assert.NoError(t, err)
		assert.NotNil(t, val)
		assert.Equal(t, []byte("value_"+strconv.Itoa(i)), val)
	}

	assert.NoError(t, err)
}

func TestFlush_EmptyMemTable(t *testing.T) {
	mmTable := setupMemTable(t, 0)

	_, err := mmTable.flush()
	assert.NoError(t, err)
}

func TestFlush_Deletes(t *testing.T) {

	mmTable := setupMemTable(t, 0)
	key := []byte("delete_me")
	value := []byte(gofakeit.LetterN(100))
	// Create namespace bucket and insert keys
	err := mmTable.db.Set(testNamespace, key, value)
	assert.NoError(t, err)

	record := walRecord{
		hlc: uint64(1),
		key: key,
		op:  wrecord.LogOperationOpDelete,
	}

	data, err := record.fbEncode()
	assert.NoError(t, err)

	walPos, err := mmTable.wal.Write(data)
	assert.NoError(t, err)

	// Store WAL ChunkPosition in MemTable
	err = mmTable.put(key, y.ValueStruct{
		Meta:  byte(wrecord.LogOperationOpDelete),
		Value: append([]byte{0}, walPos.Encode()...), // Storing WAL position
	}, walPos)

	assert.NoError(t, err)
	_, err = mmTable.flush()
	assert.NoError(t, err)

	// Verify key is deleted from BoltDB
	val, err := mmTable.db.Get(testNamespace, key)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, val)
}

func TestFlush_WALLookup(t *testing.T) {
	mmTable := setupMemTable(t, 0)

	key := []byte("wal_key")
	value := []byte("wal_value")
	record := walRecord{
		hlc:   1,
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}
	data, err := record.fbEncode()
	assert.NoError(t, err)
	walPos, err := mmTable.wal.Write(data)
	assert.NoError(t, err)

	// Store a reference to WAL instead of direct value
	err = mmTable.put(key, y.ValueStruct{
		Meta:  0,
		Value: append([]byte{0}, walPos.Encode()...),
	}, walPos)

	assert.NoError(t, err)

	// Execute flushMemTable function
	_, err = mmTable.flush()
	assert.NoError(t, err)

	// Verify the key is stored correctly in BoltDB
	val, err := mmTable.db.Get(testNamespace, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("wal_value"), val)
}

func TestProcessFlushQueue_WithTimer(t *testing.T) {
	dir := t.TempDir()
	callSignal := make(chan struct{})
	callBack := func() {
		close(callSignal)
	}
	engine, err := NewStorageEngine(dir, testNamespace, nil)
	assert.NoError(t, err, "error creating engine instance")
	engine.Callback = callBack

	key := []byte("wal_key")
	value := []byte("wal_value")

	table := newMemTable(200*wal.KB, engine.bTreeStore, engine.wal, testNamespace)

	record := walRecord{
		hlc:   1,
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}

	data, err := record.fbEncode()
	assert.NoError(t, err)

	pos, err := engine.wal.Write(data)
	assert.NoError(t, err)

	// Store WAL ChunkPosition in MemTable
	err = table.put(key, y.ValueStruct{
		Meta:  0,
		Value: append(walReferencePrefix, pos.Encode()...), // Storing WAL position
	}, pos)

	assert.NoError(t, err, "error putting data")

	engine.mu.Lock()
	engine.sealedMemTables = append(engine.sealedMemTables, table)
	engine.mu.Unlock()

	select {
	case <-callSignal:
	case <-time.After(time.Second * 10):
		t.Errorf("processFlushQueue timeout")
	}

	val, err := engine.bTreeStore.Get(testNamespace, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("wal_value"), val)

	metadata, err := LoadMetadata(engine.bTreeStore)
	assert.NoError(t, err, "error loading chunk position")
	if metadata.Pos.SegmentId != 1 {
		t.Errorf("error loading chunk position, expected 1, got %d", metadata.Pos.SegmentId)
	}

	if metadata.RecordProcessed != 1 {
		t.Errorf("error loading hlc position, expected 100, got %d", metadata.RecordProcessed)
	}
}

func TestProcessHandleFlush(t *testing.T) {
	dir := t.TempDir()
	callSignal := make(chan struct{})
	callBack := func() {
		close(callSignal)
	}
	engine, err := NewStorageEngine(dir, testNamespace, nil)
	engine.Callback = callBack
	assert.NoError(t, err, "error creating engine instance")
	key := []byte("wal_key")
	value := []byte("wal_value")

	table := newMemTable(200*wal.KB, engine.bTreeStore, engine.wal, engine.namespace)

	record := walRecord{
		hlc:   1,
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}

	data, err := record.fbEncode()
	assert.NoError(t, err)

	walPos, err := table.wal.Write(data)
	//pos := &wal.ChunkPosition{SegmentId: 1, ChunkOffset: 10}
	// Store WAL ChunkPosition in MemTable
	err = table.put(key, y.ValueStruct{
		Meta:  0,
		Value: append(directValuePrefix, data...), //direct Value
	}, walPos)

	assert.NoError(t, err, "error putting data")

	engine.mu.Lock()
	engine.sealedMemTables = append(engine.sealedMemTables, table)
	assert.Equal(t, 1, len(engine.sealedMemTables))
	engine.mu.Unlock()

	engine.handleFlush()

	select {
	case <-callSignal:
	case <-time.After(time.Second * 10):
		t.Errorf("processFlushQueue timeout")
	}

	// Verify that the key is now in BoltDB
	val, err := engine.bTreeStore.Get(testNamespace, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("wal_value"), val)

	// verify that the checkpoint has been created
	metadata, err := LoadMetadata(engine.bTreeStore)
	assert.NoError(t, err, "error loading chunk metadata")
	if metadata.Pos.SegmentId != 1 && metadata.Pos.ChunkOffset != 10 {
		t.Errorf("error loading chunk metadata, expected 10, got %d", metadata.Pos.ChunkOffset)
	}

	// after flush, the size of the sealed mem table should be one less.
	engine.mu.Lock()
	assert.Equal(t, 0, len(engine.sealedMemTables))
	engine.mu.Unlock()
}

func TestChunkFlush_Persistent(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "test_put_get"

	callSignal := make(chan struct{})
	callBack := func() {
		close(callSignal)
	}

	engine, err := NewStorageEngine(baseDir, namespace, nil)
	assert.NoError(t, err)
	engine.Callback = callBack
	t.Cleanup(func() {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	})

	key := []byte("test_key")
	value := []byte(gofakeit.Sentence(100))

	// Put key-value pair
	err = engine.Put(key, value)
	assert.NoError(t, err, "Put operation should succeed")
	assert.Equal(t, uint64(1), engine.TotalOpsReceived())
	// Retrieve value
	retrievedValue, err := engine.Get(key)
	assert.NoError(t, err, "Get operation should succeed")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match the inserted value")
	batchKey := []byte(gofakeit.Name())

	var batchValues []string
	fullValue := new(bytes.Buffer)

	for i := 0; i < 10; i++ {
		batchValues = append(batchValues, gofakeit.Sentence(5))
		fullValue.Write([]byte(batchValues[i]))
	}

	// open a batch writer:
	batch, err := engine.NewBatch(batchKey)
	assert.NoError(t, err, "NewBatch operation should succeed")
	assert.NotNil(t, batch, "NewBatch operation should succeed")

	var checksum uint32
	for _, batchValue := range batchValues {
		err := batch.Put([]byte(batchValue))
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(batchValue))
		assert.NoError(t, err, "NewBatch operation should succeed")
	}

	// get value without commit
	// write should not be visible for now.
	got, err := engine.Get(batchKey)
	assert.ErrorIs(t, err, ErrKeyNotFound, "Key not Found Error should be present.")
	assert.Nil(t, got, "Get operation should succeed")

	err = batch.Commit()
	assert.NoError(t, err, "Commit operation should succeed")

	// get value without commit
	// write should not be visible for now.
	got, err = engine.Get(batchKey)
	assert.NoError(t, err, "Get operation should succeed")
	assert.NotNil(t, got, "Get operation should succeed")
	assert.Equal(t, got, fullValue.Bytes(), "Retrieved value should match the inserted value")

	// flush the memtable.
	engine.mu.Lock()
	// put the old table in the queue
	oldTable := engine.currentMemTable
	engine.currentMemTable = newMemTable(engine.storageConfig.ArenaSize, engine.bTreeStore, engine.wal, testNamespace)
	engine.sealedMemTables = append(engine.sealedMemTables, oldTable)
	engine.mu.Unlock()

	select {
	case engine.flushSignal <- struct{}{}:
	default:
		slog.Warn("queue signal channel full")
	}

	select {
	case <-callSignal:
	case <-time.After(time.Second * 10):
		t.Errorf("processFlushQueue timeout")
	}

	got, err = engine.Get(batchKey)
	assert.NoError(t, err, "Get operation should succeed")
	assert.NotNil(t, got, "Get operation should succeed")
	assert.Equal(t, got, fullValue.Bytes(), "Retrieved value should match the inserted value")

}
