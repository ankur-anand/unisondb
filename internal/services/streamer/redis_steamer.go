package streamer

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/pkg/replicator"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const (
	requestQueue = "wal:requests:list"
	messageTTL   = 2 * time.Minute
)

type RedisStreamerServer struct {
	redisClient    *redis.Client
	storageEngines map[string]*dbkernel.Engine
}

type StreamMessage struct {
	Type    string `json:"type"`              // "record" or "error"
	Payload string `json:"payload,omitempty"` // base64 WALRecord
	Offset  string `json:"offset,omitempty"`
	Error   string `json:"error,omitempty"`
}

func NewRedisStreamerServer(redisClient *redis.Client, engines map[string]*dbkernel.Engine) *RedisStreamerServer {
	return &RedisStreamerServer{
		redisClient:    redisClient,
		storageEngines: engines,
	}
}

func (s *RedisStreamerServer) Start(ctx context.Context) {
	ctxDoneSignal := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(ctxDoneSignal)

	}()
	for {
		select {
		case <-ctxDoneSignal:
			return
		default:
			result, err := s.redisClient.BLPop(ctx, 0, requestQueue).Result()
			if err != nil || len(result) != 2 {
				continue
			}
			request := result[1]
			go s.handleRequest(ctx, request)
		}
	}
}

func (s *RedisStreamerServer) handleRequest(ctx context.Context, payload string) {
	var clientID, namespace, encodedOffset string
	_, err := fmt.Sscanf(payload, "client_id=%s namespace=%s offset=%s", &clientID, &namespace, &encodedOffset)
	if err != nil {
		log.Printf("invalid request payload: %s", payload)
		return
	}
	offsetData, err := base64.StdEncoding.DecodeString(encodedOffset)
	if err != nil {
		log.Printf("invalid offset: %s", encodedOffset)
		return
	}
	offset, _ := decodeOffset(offsetData)

	engine, ok := s.storageEngines[namespace]
	if !ok {
		log.Printf("namespace not found: %s", namespace)
		return
	}

	rep := replicator.NewReplicator(engine, 20, 100*time.Millisecond, offset, "redis")
	walChan := make(chan []*v1.WALRecord, 100)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	replicatorErr := make(chan error, 1)
	go func() {
		err := rep.Replicate(ctx, walChan)
		select {
		case replicatorErr <- err:
		case <-ctx.Done():
			return
		}
	}()

	queueKey := fmt.Sprintf("wal:queue:%s", clientID)

	for {
		select {
		case batch, ok := <-walChan:
			if !ok {
				return
			}
			pipe := s.redisClient.Pipeline()
			for _, record := range batch {
				data, err := proto.Marshal(record)
				if err != nil {
					continue
				}
				msg := StreamMessage{
					Type:    "record",
					Payload: base64.StdEncoding.EncodeToString(data),
					Offset:  base64.StdEncoding.EncodeToString(record.Offset),
				}
				jsonMsg, _ := json.Marshal(msg)
				pipe.RPush(ctx, queueKey, jsonMsg)
			}
			pipe.Expire(ctx, queueKey, messageTTL)
			_, _ = pipe.Exec(ctx)

		case err := <-replicatorErr:
			if err != nil {
				log.Printf("[redis.streamer] Replicator error for namespace %s: %v", namespace, err)
				errMsg := StreamMessage{
					Type:  "error",
					Error: fmt.Sprintf("replication failed: %v", err),
				}
				jsonMsg, _ := json.Marshal(errMsg)
				_ = s.redisClient.LPush(ctx, queueKey, jsonMsg).Err()
				if errors.Is(err, dbkernel.ErrInvalidOffset) {
					log.Printf("[redis.streamer] invalid offset received: %v", err)
					return
				}
				return
			}
			return
		case <-ctx.Done():
			return
		}
	}
}

func decodeOffset(data []byte) (*dbkernel.Offset, error) {
	defer func() {
		_ = recover()
	}()
	if len(data) == 0 {
		return nil, nil
	}
	return dbkernel.DecodeOffset(data), nil
}
