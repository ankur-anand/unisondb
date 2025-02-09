package storage

import (
	"bytes"
	"errors"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/uuid"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestFlusherQueue(t *testing.T) {

	signal := make(chan struct{})
	enq := newFlusherQueue(signal)

	if enq.dequeue() != nil {
		t.Error("enq dequeue should return nil for empty queue")
	}

	type mCase struct {
		memTable *skl.Skiplist
		id       []byte
	}

	var memTables []mCase
	key := y.KeyWithTs([]byte("uuid"), 0)

	for i := 0; i <= 5; i++ {
		id, err := uuid.New().MarshalText()
		if err != nil {
			t.Fatal(err)
		}
		memTables = append(memTables, mCase{
			memTable: skl.NewSkiplist(2 * wal.MB),
			id:       id,
		})

		memTables[i].memTable.Put(key, y.ValueStruct{
			Meta:      0,
			UserMeta:  0,
			ExpiresAt: 0,
			Value:     id,
			Version:   0,
		})

		enq.enqueue(memTable{
			sList:       memTables[i].memTable,
			currentPost: nil,
		})
	}

	for i := 0; i <= 5; i++ {
		mt := enq.dequeue()
		exp := memTables[i].id

		if bytes.Compare(mt.sList.Get(key).Value, exp) != 0 {
			t.Errorf("enq dequeue = %v; want %v", mt, exp)
		}
	}
}

func TestMemTable_PutAndGet(t *testing.T) {

	const capacity = wal.MB
	table := newMemTable(capacity)

	// Create a test key, value, and WAL position.
	key := []byte("test-key")
	val := y.ValueStruct{Value: []byte("test-value")}
	pos := &wal.ChunkPosition{SegmentId: 1}

	// Verify that canPut returns true initially.
	if !table.canPut(key, val) {
		t.Fatalf("expected canPut to return true for key %q", key)
	}

	err := table.put(key, val, pos, 1)
	assert.NoError(t, err, "unexpected error on put")

	gotVal := table.get(key)
	// Use reflect.DeepEqual (or a more specific comparison) to check equality.
	if bytes.Compare(gotVal.Value, val.Value) != 0 {
		t.Errorf("got value %+v, expected %+v", gotVal, val)
	}

	// Verify that the table's WAL position was updated.
	if table.currentPost != pos {
		t.Errorf("expected table currentPost to be %+v, got %+v", pos, table.currentPost)
	}
}

func TestMemTable_CannotPut(t *testing.T) {
	const capacity = 1 * wal.KB // a very small arena size for testing
	table := newMemTable(capacity)

	key := []byte("key")
	// more than 1 KB
	value := gofakeit.LetterN(1100)
	val := y.ValueStruct{Value: []byte(value)}
	pos := &wal.ChunkPosition{SegmentId: 1}

	// should not panic
	err := table.put(key, val, pos, 1)
	if !errors.Is(err, errArenaSizeWillExceed) {
		t.Fatalf("expected error %q, got %v", errArenaSizeWillExceed, err)
	}
}

func TestFlush_Success(t *testing.T) {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := bbolt.Open(dbFile, 0600, nil)
	assert.NoError(t, err)
	defer func(db *bbolt.DB) {
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close db: %s", err)
		}
	}(db)

	memTable := skl.NewSkiplist(1 * 1024 * 1024)

	// Create namespace bucket in BoltDB
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("test_namespace"))
		return err
	})
	assert.NoError(t, err)

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.Open(newWALOptions(walDir))
	assert.NoError(t, err)
	defer walInstance.Close()

	for i := 0; i < 150; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))

		record := walRecord{
			index: uint64(i),
			key:   key,
			value: value,
			op:    wrecord.LogOperationOpInsert,
		}

		encodedRecord, err := record.fbEncode()
		assert.NoError(t, err, "failed to encode record")
		walPos, err := walInstance.Write(encodedRecord)
		assert.NoError(t, err)

		// Store WAL ChunkPosition in MemTable
		memTable.Put(y.KeyWithTs(key, 0), y.ValueStruct{
			Meta:  byte(wrecord.LogOperationOpInsert),
			Value: append([]byte{0}, walPos.Encode()...), // Storing WAL position
		})
	}

	err = flushMemTable("test_namespace", memTable, db, walInstance)
	assert.NoError(t, err)

	// **Verify
	for i := 0; i < 150; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		val, err := retrieveFromBoltDB("test_namespace", db, key)
		assert.NoError(t, err)
		assert.NotNil(t, val)
		assert.Equal(t, []byte("value_"+strconv.Itoa(i)), val)
	}

	assert.NoError(t, err)
}

func TestFlush_EmptyMemTable(t *testing.T) {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := bbolt.Open(dbFile, 0600, nil)
	assert.NoError(t, err)
	defer db.Close()

	memTable := skl.NewSkiplist(1 * 1024 * 1024)

	// Create namespace bucket in BoltDB
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("test_namespace"))
		return err
	})
	assert.NoError(t, err)
	walDir := filepath.Join(dir, "wal_test")
	walInstance, err := wal.Open(newWALOptions(walDir))
	assert.NoError(t, err)
	defer walInstance.Close()

	err = flushMemTable("test_namespace", memTable, db, walInstance)
	assert.NoError(t, err)
}

func TestFlush_Deletes(t *testing.T) {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := bbolt.Open(dbFile, 0600, nil)
	assert.NoError(t, err)
	defer func(db *bbolt.DB) {
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close db: %s", err)
		}
	}(db)

	memTable := skl.NewSkiplist(1 * 1024 * 1024)

	// Create namespace bucket in BoltDB
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("test_namespace"))
		return err
	})
	assert.NoError(t, err)

	tdir := os.TempDir()
	walDir := filepath.Join(tdir, "wal_test")
	err = os.MkdirAll(walDir, 0777)
	assert.NoError(t, err)

	walInstance, err := wal.Open(newWALOptions(walDir))
	assert.NoError(t, err)
	defer walInstance.Close()
	// Create namespace bucket and insert keys
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("test_namespace"))
		assert.NoError(t, err)
		assert.NotNil(t, bucket)

		err = bucket.Put([]byte("delete_me"), []byte("to_be_deleted"))
		assert.NoError(t, err)
		return nil
	})
	assert.NoError(t, err)

	record := walRecord{
		index: uint64(1),
		key:   []byte("delete_me"),
		op:    wrecord.LogOperationOpDelete,
	}

	data, err := record.fbEncode()
	assert.NoError(t, err)

	walPos, err := walInstance.Write(data)
	assert.NoError(t, err)

	// Store WAL ChunkPosition in MemTable
	memTable.Put(y.KeyWithTs([]byte("delete_me"), 0), y.ValueStruct{
		Meta:  byte(wrecord.LogOperationOpDelete),
		Value: append([]byte{0}, walPos.Encode()...), // Storing WAL position
	})

	err = flushMemTable("test_namespace", memTable, db, walInstance)
	assert.NoError(t, err)

	// Verify key is deleted from BoltDB
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("test_namespace"))
		assert.NotNil(t, bucket)
		assert.Nil(t, bucket.Get([]byte("delete_me"))) // Ensure it's deleted
		return nil
	})
	assert.NoError(t, err)
}

func TestFlush_WALLookup(t *testing.T) {
	dir := t.TempDir()

	dbFile := filepath.Join(dir, "test_flush.db")

	db, err := bbolt.Open(dbFile, 0600, nil)
	assert.NoError(t, err)
	defer db.Close()

	memTable := skl.NewSkiplist(1 * 1024 * 1024)

	// Create namespace bucket in BoltDB
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("test_namespace"))
		return err
	})
	assert.NoError(t, err)
	walDir := filepath.Join(dir, "wal_test")
	walInstance, err := wal.Open(newWALOptions(walDir))
	assert.NoError(t, err)
	defer walInstance.Close()

	key := []byte("wal_key")
	value := []byte("wal_value")
	record := walRecord{
		index: 1,
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}
	data, err := record.fbEncode()
	assert.NoError(t, err)
	walPos, err := walInstance.Write(data)
	assert.NoError(t, err)

	// Store a reference to WAL instead of direct value
	memTable.Put(y.KeyWithTs(key, 0), y.ValueStruct{
		Meta:  0,
		Value: append([]byte{0}, walPos.Encode()...),
	})

	// Execute flushMemTable function
	err = flushMemTable("test_namespace", memTable, db, walInstance)
	assert.NoError(t, err)

	// Verify the key is stored correctly in BoltDB
	val, err := retrieveFromBoltDB("test_namespace", db, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("wal_value"), val)
}

func TestProcessFlushQueue_WithTimer(t *testing.T) {
	dir := t.TempDir()
	callSignal := make(chan struct{})
	callBack := func() {
		close(callSignal)
	}
	engine, err := NewStorageEngine(dir, "test_namespace", nil)
	assert.NoError(t, err, "error creating engine instance")
	engine.Callback = callBack

	key := []byte("wal_key")
	value := []byte("wal_value")

	table := newMemTable(200 * wal.KB)

	record := walRecord{
		index: 1,
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}
	data, err := record.fbEncode()
	assert.NoError(t, err)
	pos := &wal.ChunkPosition{SegmentId: 1}
	// Store WAL ChunkPosition in MemTable
	err = table.put(key, y.ValueStruct{
		Meta:  0,
		Value: append([]byte{1}, data...), // Storing WAL position
	}, pos, 100)

	assert.NoError(t, err, "error putting data")

	engine.flushQueue.enqueue(*table)

	signal := make(chan struct{})
	var wg sync.WaitGroup

	engine.processFlushQueue(&wg, signal)

	select {
	case <-callSignal:
	case <-time.After(time.Second * 10):
		t.Errorf("processFlushQueue timeout")
	}

	val, err := retrieveFromBoltDB(engine.namespace, engine.db, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("wal_value"), val)

	// Cleanup
	close(signal)
	wg.Wait()

	metadata, err := LoadMetadata(engine.db)
	assert.NoError(t, err, "error loading chunk position")
	if metadata.Pos.SegmentId != 1 {
		t.Errorf("error loading chunk position, expected 1, got %d", metadata.Pos.SegmentId)
	}

	if metadata.Index != 100 {
		t.Errorf("error loading index position, expected 100, got %d", metadata.Index)
	}
}

func TestProcessFlushQueue(t *testing.T) {
	dir := t.TempDir()
	callSignal := make(chan struct{})
	callBack := func() {
		close(callSignal)
	}
	engine, err := NewStorageEngine(dir, "test_namespace", nil)
	engine.Callback = callBack
	assert.NoError(t, err, "error creating engine instance")
	key := []byte("wal_key")
	value := []byte("wal_value")

	table := newMemTable(200 * wal.KB)

	record := walRecord{
		index: 1,
		key:   key,
		value: value,
		op:    wrecord.LogOperationOpInsert,
	}
	// compress
	data, err := record.fbEncode()
	assert.NoError(t, err)
	pos := &wal.ChunkPosition{SegmentId: 1, ChunkOffset: 10}
	// Store WAL ChunkPosition in MemTable
	err = table.put(key, y.ValueStruct{
		Meta:  0,
		Value: append([]byte{1}, data...), // Storing WAL metadata
	}, pos, 1)

	assert.NoError(t, err, "error putting data")

	engine.flushQueue.enqueue(*table)

	signal := make(chan struct{})
	var wg sync.WaitGroup

	engine.processFlushQueue(&wg, signal)

	// Trigger
	signal <- struct{}{}

	select {
	case <-callSignal:
	case <-time.After(time.Second * 10):
		t.Errorf("processFlushQueue timeout")
	}

	// Verify that the key is now in BoltDB
	val, err := retrieveFromBoltDB(engine.namespace, engine.db, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("wal_value"), val)

	// Cleanup
	close(signal)
	wg.Wait()

	// verify that the checkpoint has been created
	metadata, err := LoadMetadata(engine.db)
	assert.NoError(t, err, "error loading chunk metadata")
	if metadata.Pos.SegmentId != 1 && metadata.Pos.ChunkOffset != 10 {
		t.Errorf("error loading chunk metadata, expected 10, got %d", metadata.Pos.ChunkOffset)
	}
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
	defer func(engine *Engine) {
		err := engine.Close()
		if err != nil {
			t.Errorf("Failed to close engine: %v", err)
		}
	}(engine)

	key := []byte("test_key")
	value := []byte(gofakeit.Sentence(100))

	// Put key-value pair
	err = engine.Put(key, value)
	assert.NoError(t, err, "Put operation should succeed")
	assert.Equal(t, uint64(1), engine.LastSeq())
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
	oldTable := engine.memTable
	engine.memTable = newMemTable(engine.storageConfig.ArenaSize)
	engine.flushQueue.enqueue(*oldTable)
	select {
	case engine.queueChan <- struct{}{}:
	default:
		slog.Warn("queue signal channel full")
	}
	engine.mu.Unlock()

	signal := make(chan struct{})
	var wg sync.WaitGroup

	engine.processFlushQueue(&wg, signal)

	// Trigger
	signal <- struct{}{}

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
