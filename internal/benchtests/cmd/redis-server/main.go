package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/benchtests/cmd/redis-server/store"
	"github.com/ankur-anand/unisondb/internal/etc"
	"github.com/hashicorp/go-metrics"
	"github.com/tidwall/redcon"
)

type Storage interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Close() error
	TotalOpsCount() uint64
}

var (
	dataDir   = flag.String("dir", "./data", "data directory")
	namespace = flag.String("namespace", "default", "namespace")
	port      = flag.Int("port", 6380, "server port")
	engine    = flag.String("engine", "unison", "database engine")
)

func main() {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()
	var se Storage
	var err error
	switch *engine {
	case "unison":
		se, err = store.NewUnisonDB(*dataDir, *namespace)
	case "badger":
		se, err = store.NewBadgerStore(*dataDir)
	case "bolt":
		se, err = store.NewBoltStore(*namespace, *dataDir)
	case "lmdb":
		se, err = store.NewLMDBStore(*namespace, *dataDir)
	default:
		log.Fatalf("unknown engine: %s", *engine)
	}

	if err != nil {
		log.Fatal(err)
	}

	defer se.Close()

	if *engine == "unison" {
		if unisonStore, ok := se.(*store.KVAlchemy); ok {
			go func() {
				ticker := time.NewTicker(10 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						count := unisonStore.SealedMemTableCount()
						slog.Info("sealed memtable status",
							"count", count,
							"ops_received", unisonStore.TotalOpsCount())
					}
				}
			}()
		}
	}

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	addr := net.JoinHostPort("", fmt.Sprintf("%d", *port))

	go slog.Info("started server", "addr", addr, "engine", *engine, "dataDir", *dataDir)
	inm := metrics.NewInmemSink(10*time.Second, time.Minute)
	cfg := metrics.DefaultConfig("benchmark")
	cfg.EnableHostname = false
	cfg.EnableRuntimeMetrics = true
	_, err = metrics.NewGlobal(cfg, inm)
	if err != nil {
		panic(err)
	}

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
				case "ops":
					conn.WriteUint64(se.TotalOpsCount())
				case "set":
					if len(cmd.Args) != 3 {
						conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
						return
					}
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

					if errors.Is(err, dbkernel.ErrKeyNotFound) {
						conn.WriteNull()
						return
					}

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

					conn.WriteInt(1)
				case "config":
					// This simple (blank) response is only here to allow for the
					// redis-benchmark command to work with this example.
					conn.WriteArray(2)
					conn.WriteBulk(cmd.Args[2])
					conn.WriteBulkString("")
				case "metrics":
					etc.DumpStats(inm, os.Stdout)
					conn.WriteNull()
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
