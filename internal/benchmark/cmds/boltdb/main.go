package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"go.etcd.io/bbolt"
)

var (
	bucket = []byte("test_bucket")
)

func generateRandomKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = gofakeit.UUID()
	}
	return keys
}

func randomInsert(db *bbolt.DB, n int) time.Duration {
	keys := generateRandomKeys(n)
	start := time.Now()
	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, key := range keys {
			b.Put([]byte(key), []byte("value"))
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return time.Since(start)
}

func sortedInsert(db *bbolt.DB, n int) time.Duration {
	keys := generateRandomKeys(n)
	sort.Strings(keys)
	start := time.Now()
	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, key := range keys {
			b.Put([]byte(key), []byte("value"))
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return time.Since(start)
}

func batchInsertSorted(db *bbolt.DB, n int, batchSize int) time.Duration {
	keys := generateRandomKeys(n)
	sort.Strings(keys)
	start := time.Now()
	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for i := 0; i < n; i += batchSize {
			for j := i; j < i+batchSize && j < n; j++ {
				b.Put([]byte(keys[j]), []byte("value"))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return time.Since(start)
}

func batchInsertRandom(db *bbolt.DB, n int, batchSize int) time.Duration {
	keys := generateRandomKeys(n)
	start := time.Now()
	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for i := 0; i < n; i += batchSize {
			for j := i; j < i+batchSize && j < n; j++ {
				b.Put([]byte(keys[j]), []byte("value"))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return time.Since(start)
}

func getKeyCount(db *bbolt.DB) int {
	var keyCount int
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		keyCount = b.Stats().KeyN
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return keyCount
}

func main() {
	sizes := []int{10000, 50000, 100000}
	pageSizes := []int{4096}

	file, err := os.Create("benchmark_results.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"Size", "Page Size", "Key Count", "random", "sorted", "batch_sorted", "batch_random"})

	for _, pageSize := range pageSizes {
		for _, size := range sizes {
			dbFileName := fmt.Sprintf("benchmark_%d.db", pageSize)
			db, err := bbolt.Open(dbFileName, 0600, &bbolt.Options{PageSize: pageSize})
			if err != nil {
				log.Fatal(err)
			}

			randomTime := randomInsert(db, size).Seconds()
			sortedTime := sortedInsert(db, size).Seconds()
			batchSortedTime := batchInsertSorted(db, size, 50).Seconds()
			batchRandomTime := batchInsertRandom(db, size, 50).Seconds()
			keyCount := getKeyCount(db)
			db.Close()

			fmt.Printf("Size: %d, Page Size: %d, Key Count: %d\n", size, pageSize, keyCount)
			fmt.Printf("Random Insert: %.4f sec\n", randomTime)
			fmt.Printf("Sorted Insert: %.4f sec\n", sortedTime)
			fmt.Printf("Batch Insert Sorted: %.4f sec\n", batchSortedTime)
			fmt.Printf("Batch Insert Random: %.4f sec\n", batchRandomTime)

			writer.Write([]string{
				fmt.Sprintf("%d", size),
				fmt.Sprintf("%d", pageSize),
				fmt.Sprintf("%d", keyCount),
				fmt.Sprintf("%.4f", randomTime),
				fmt.Sprintf("%.4f", sortedTime),
				fmt.Sprintf("%.4f", batchSortedTime),
				fmt.Sprintf("%.4f", batchRandomTime),
			})
		}
	}
}
