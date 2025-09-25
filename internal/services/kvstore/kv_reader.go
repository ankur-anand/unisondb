package kvstore

import (
	"errors"
	"hash/crc32"

	storage "github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services"
	"github.com/ankur-anand/unisondb/pkg/splitter"
	v2 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
	"google.golang.org/grpc"
)

type KVReaderService struct {
	storageEngines map[string]*storage.Engine
	v2.UnimplementedKVStoreReadServiceServer
}

func NewKVReaderService(engine map[string]*storage.Engine) *KVReaderService {
	return &KVReaderService{
		storageEngines: engine,
	}
}

func (k *KVReaderService) Get(request *v2.GetRequest, g grpc.ServerStreamingServer[v2.GetResponse]) error {
	namespace, reqID, method := grpcutils.GetRequestInfo(g.Context())

	if namespace == "" {
		return services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := k.storageEngines[namespace]
	if !ok {
		return services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	value, err := engine.GetKV(request.GetKey())
	if errors.Is(err, storage.ErrKeyNotFound) {
		return services.ErrKeyNotFound
	}

	if err != nil {
		return services.ToGRPCError(namespace, reqID, method, err)
	}

	// if the value is too large then the
	if len(value) < capValueSize {
		err := g.SendMsg(&v2.GetResponse{
			Data:               value,
			FinalCrc32Checksum: crc32.ChecksumIEEE(value),
			Chunked:            false,
		})
		if err != nil {
			return services.ToGRPCError(namespace, reqID, method, err)
		}
		return nil
	}

	checksum := crc32.ChecksumIEEE(value)
	chunks := splitter.SplitIntoChunks(value)
	for _, chunk := range chunks {
		err := g.SendMsg(&v2.GetResponse{
			Data:               chunk.Data,
			FinalCrc32Checksum: checksum,
			Chunked:            true,
		})
		if err != nil {
			return services.ToGRPCError(namespace, reqID, method, err)
		}
	}

	return nil
}
