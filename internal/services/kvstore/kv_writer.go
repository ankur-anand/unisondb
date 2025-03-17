package kvstore

import (
	"context"
	"errors"
	"io"
	"time"

	storage "github.com/ankur-anand/unisondb/dbengine"
	"github.com/ankur-anand/unisondb/dbengine/wal/walrecord"
	"github.com/ankur-anand/unisondb/internal/middleware"
	"github.com/ankur-anand/unisondb/internal/services"
	v1 "github.com/ankur-anand/unisondb/proto/gen/go/kvalchemy/replicator/v1"
	"google.golang.org/grpc"
)

// Timeout duration for stream handling.
const streamTimeout = 15 * time.Minute

type KVWriterService struct {
	storageEngines map[string]*storage.Engine
	v1.UnimplementedKVStoreWriteServiceServer
}

func NewKVWriterService(engines map[string]*storage.Engine) *KVWriterService {
	return &KVWriterService{
		storageEngines: engines,
	}
}

func (k *KVWriterService) Put(ctx context.Context, request *v1.PutRequest) (*v1.PutResponse, error) {
	namespace, reqID, method := middleware.GetRequestInfo(ctx)
	if namespace == "" {
		return nil, services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := k.storageEngines[namespace]
	if !ok {
		return nil, services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	if err := engine.Put(request.Key, request.Value); err != nil {
		return nil, services.ToGRPCError(namespace, reqID, method, err)
	}

	return &v1.PutResponse{}, nil
}

func (k *KVWriterService) PutStream(g grpc.ClientStreamingServer[v1.PutStreamRequest, v1.PutStreamResponse]) error {
	namespace, reqID, method := middleware.GetRequestInfo(g.Context())
	if namespace == "" {
		return services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := k.storageEngines[namespace]
	if !ok {
		return services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	for {
		msg, err := g.Recv()
		if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return services.ToGRPCError(namespace, reqID, method, err)
		}

		for _, putReq := range msg.KvPairs {
			if err := engine.Put(putReq.Key, putReq.Value); err != nil {
				return services.ToGRPCError(namespace, reqID, method, err)
			}
		}
	}
}

func (k *KVWriterService) PutStreamChunksForKey(g grpc.ClientStreamingServer[v1.PutStreamChunksForKeyRequest, v1.PutStreamChunksForKeyResponse]) error {
	ctx, cancel := context.WithTimeout(g.Context(), streamTimeout)
	defer cancel()

	namespace, reqID, method := middleware.GetRequestInfo(g.Context())
	if namespace == "" {
		return services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := k.storageEngines[namespace]
	if !ok {
		return services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	var txn *storage.Txn
	var committed bool
	var key []byte
	for {
		select {
		case <-ctx.Done():
			return services.ToGRPCError(namespace, reqID, method, context.DeadlineExceeded)
		default:
			msg, err := g.Recv()
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
					return nil
				}
				return services.ToGRPCError(namespace, reqID, method, err)
			}

			if committed {
				return services.ToGRPCError(namespace, reqID, method, services.ErrPutChunkAlreadyCommited)
			}

			switch req := msg.GetRequestType().(type) {
			case *v1.PutStreamChunksForKeyRequest_StartMarker:
				key = req.StartMarker.GetKey()
				txn, err = k.handleStartMarker(engine, g, req)
			case *v1.PutStreamChunksForKeyRequest_Chunk:
				err = k.handleChunk(txn, key, req)
			case *v1.PutStreamChunksForKeyRequest_CommitMarker:
				err = k.handleCommitMarker(txn, req)
				committed = err == nil
			}

			if err != nil {
				return services.ToGRPCError(namespace, reqID, method, err)
			}
		}
	}
}

func (k *KVWriterService) handleStartMarker(engine *storage.Engine,
	g grpc.ClientStreamingServer[v1.PutStreamChunksForKeyRequest, v1.PutStreamChunksForKeyResponse],
	req *v1.PutStreamChunksForKeyRequest_StartMarker) (*storage.Txn, error) {
	batch, err := engine.NewTxn(walrecord.LogOperationInsert, walrecord.EntryTypeChunked)
	if err != nil {
		return nil, err
	}

	if err := g.SendMsg(&v1.PutStreamChunksForKeyResponse{}); err != nil {
		return nil, err
	}

	return batch, nil
}

func (k *KVWriterService) handleChunk(txn *storage.Txn, key []byte, req *v1.PutStreamChunksForKeyRequest_Chunk) error {
	if txn == nil {
		return services.ErrPutChunkPrecondition
	}

	value := req.Chunk.GetValue()
	return txn.AppendKVTxn(key, value)
}

func (k *KVWriterService) handleCommitMarker(txn *storage.Txn,
	req *v1.PutStreamChunksForKeyRequest_CommitMarker) error {
	if txn == nil {
		return services.ErrPutChunkPrecondition
	}

	checksum := req.CommitMarker.GetFinalCrc32Checksum()
	if txn.Checksum() != checksum {
		return services.ErrPutChunkCheckSumMismatch
	}

	return txn.Commit()
}

func (k *KVWriterService) Delete(ctx context.Context, request *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	namespace, reqID, method := middleware.GetRequestInfo(ctx)
	if namespace == "" {
		return nil, services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := k.storageEngines[namespace]
	if !ok {
		return nil, services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}
	if err := engine.Delete(request.Key); err != nil {
		return nil, services.ToGRPCError(namespace, reqID, method, err)
	}

	return &v1.DeleteResponse{}, nil
}

func (k *KVWriterService) DeleteStream(g grpc.ClientStreamingServer[v1.DeleteStreamRequest, v1.DeleteStreamResponse]) error {
	namespace, reqID, method := middleware.GetRequestInfo(g.Context())
	if namespace == "" {
		return services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := k.storageEngines[namespace]
	if !ok {
		return services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	for {
		msg, err := g.Recv()
		if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return services.ToGRPCError(namespace, reqID, method, err)
		}

		for _, delReq := range msg.Deletes {
			if err := engine.Delete(delReq.Key); err != nil {
				return services.ToGRPCError(namespace, reqID, method, err)
			}
		}
	}
}
