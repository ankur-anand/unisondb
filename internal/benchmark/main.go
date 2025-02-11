package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ankur-anand/kvalchemy/internal/benchmark/store"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/tidwall/redcon"
)

type Storage interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
	TotalOpsCount() uint64
}

var (
	dataDir   = flag.String("dir", "./data", "data directory")
	namespace = flag.String("namespace", "default", "namespace")
	port      = flag.Int("port", 6380, "server port")
	engine    = flag.String("engine", "alchemy", "database engine")
)

func main() {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()
	var se Storage
	var err error
	switch *engine {
	case "alchemy":
		se, err = store.NewKVAlchemy(*dataDir, *namespace)
	case "badger":
		se, err = store.NewBadgerStore(*dataDir)
	case "bolt":
		se, err = store.NewBoltStore(*namespace, *dataDir)
	default:
		log.Fatalf("unknown engine: %s", *engine)
	}

	if err != nil {
		log.Fatal(err)
	}

	defer se.Close()

	addr := net.JoinHostPort("", fmt.Sprintf("%d", *port))

	go slog.Info("started server", "addr", addr, "engine", *engine, "dataDir", *dataDir)
	var setCount int
	bMutex := &sync.Mutex{}
	bloomFilter := bloom.NewWithEstimates(1_000_000, 0.0001)

	go func() {
		err = redcon.ListenAndServe(addr,
			func(conn redcon.Conn, cmd redcon.Command) {
				switch strings.ToLower(string(cmd.Args[0])) {
				default:
					conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
				case "ping":
					conn.WriteString("PONG")
				case "quit":
					conn.WriteString("OK")
					conn.Close()
				case "set_count":
					bMutex.Lock()
					conn.WriteInt(setCount)
					bMutex.Unlock()
				case "ops_count":
					conn.WriteUint64(se.TotalOpsCount())
				case "set":
					if len(cmd.Args) != 3 {
						conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
						return
					}
					bMutex.Lock()
					if !bloomFilter.Test([]byte(cmd.Args[1])) {
						setCount++
					}
					bMutex.Unlock()
					err := se.Set(cmd.Args[1], cmd.Args[2])
					if err != nil {
						slog.Error("failed to set", "key", cmd.Args[1], "value", cmd.Args[2], "err", err)
						conn.WriteError(err.Error())
						return
					}
					conn.WriteString("OK")
				case "get":
					if len(cmd.Args) != 2 {
						conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
						return
					}

					val, err := se.Get(cmd.Args[1])

					if err != nil {
						conn.WriteError(err.Error())
						return
					}

					if len(val) == 0 {
						conn.WriteNull()
					} else {
						conn.WriteBulk(val)
					}
				case "del":
					if len(cmd.Args) != 2 {
						conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
						return
					}
					err := se.Delete(cmd.Args[1])
					if err != nil {
						conn.WriteError(err.Error())
						return
					}

					conn.WriteInt(1)
				case "config":
					// This simple (blank) response is only here to allow for the
					// redis-benchmark command to work with this example.
					conn.WriteArray(2)
					conn.WriteBulk(cmd.Args[2])
					conn.WriteBulkString("")
				}
			},
			func(conn redcon.Conn) bool {
				// Use this function to accept or deny the connection.
				// log.Printf("accept: %s", conn.RemoteAddr())
				return true
			},
			func(conn redcon.Conn, err error) {
				// This is called when the connection has been closed
				// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
			},
		)

		if err != nil {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	err = se.Close()
	if err != nil {
		slog.Error(err.Error())
	}

	time.Sleep(10 * time.Second)
	log.Println("closing")
}
