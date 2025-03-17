package kvstore

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/ankur-anand/unisondb/internal/services"
	"github.com/ankur-anand/unisondb/pkg/splitter"
	v2 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	capValueSize = 1 * 1024 * 1024
)

var (
	ErrValueSizeLimitExceeded = errors.New("value size limit exceeded 1 MB")
	ErrKeyNotFound            = errors.New("key not found")
	ErrMissingParameters      = errors.New("missing mandatory parameters [namespace|key]")
)

type Client struct {
	gcc          *grpc.ClientConn
	writerClient v2.KVStoreWriteServiceClient
	readerClient v2.KVStoreReadServiceClient
}

func NewClient(gcc *grpc.ClientConn) *Client {
	return &Client{
		gcc:          gcc,
		writerClient: v2.NewKVStoreWriteServiceClient(gcc),
		readerClient: v2.NewKVStoreReadServiceClient(gcc),
	}
}

// PutKV stores the given key-value pair in the specified namespace via gRPC.
func (c *Client) PutKV(ctx context.Context, namespace, key string, value []byte) error {
	if len(value) > capValueSize {
		return ErrValueSizeLimitExceeded
	}
	if namespace == "" || key == "" {
		return ErrMissingParameters
	}

	md := metadata.Pairs("x-namespace", namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)

	_, err := c.writerClient.Put(ctx, &v2.PutRequest{
		Key:   []byte(key),
		Value: value,
	})

	if err != nil {
		cErr := status.Convert(err)
		return errors.New(cErr.Message())
	}

	return nil
}

func (c *Client) DeleteKV(ctx context.Context, namespace, key string) error {
	if namespace == "" || key == "" {
		return ErrMissingParameters
	}
	md := metadata.Pairs("x-namespace", namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)

	_, err := c.writerClient.Delete(ctx, &v2.DeleteRequest{
		Key: []byte(key),
	})

	if err != nil {
		cErr := status.Convert(err)
		return errors.New(cErr.Message())
	}

	return nil
}

func (c *Client) PutStreamChunksForKey(ctx context.Context, namespace, key string, chunks [][]byte) error {
	if len(chunks) == 0 {
		return errors.New("chunks is empty")
	}
	if namespace == "" || key == "" {
		return ErrMissingParameters
	}

	for _, chunk := range chunks {
		if len(chunk) > capValueSize {
			return ErrValueSizeLimitExceeded
		}
	}

	md := metadata.Pairs("x-namespace", namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)
	stream, err := c.writerClient.PutStreamChunksForKey(ctx)
	if err != nil {
		cErr := status.Convert(err)
		return errors.New(cErr.Message())
	}

	err = stream.Send(&v2.PutStreamChunksForKeyRequest{
		RequestType: &v2.PutStreamChunksForKeyRequest_StartMarker{StartMarker: &v2.ChunkStartMarker{Key: []byte(key)}},
	})

	if err != nil {
		cErr := status.Convert(err)
		return errors.New(cErr.Message())
	}

	checksum := uint32(0)
	for _, chunk := range chunks {
		err = stream.Send(&v2.PutStreamChunksForKeyRequest{
			RequestType: &v2.PutStreamChunksForKeyRequest_Chunk{Chunk: &v2.ChunkPutValue{Value: chunk}},
		})
		checksum = crc32.Update(checksum, crc32.IEEETable, chunk)
		if err != nil {
			cErr := status.Convert(err)
			return errors.New(cErr.Message())
		}
	}

	// commit
	err = stream.Send(&v2.PutStreamChunksForKeyRequest{
		RequestType: &v2.PutStreamChunksForKeyRequest_CommitMarker{CommitMarker: &v2.ChunkCommitMarker{
			FinalCrc32Checksum: checksum,
		}},
	})
	if err != nil {
		cErr := status.Convert(err)
		return errors.New(cErr.Message())
	}

	// close the stream and wait for it to finish,
	_, err = stream.CloseAndRecv()
	return err
}

func (c *Client) GetKV(ctx context.Context, namespace, key string) ([]byte, error) {
	if namespace == "" || key == "" {
		return nil, ErrMissingParameters
	}
	md := metadata.Pairs("x-namespace", namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)
	response, err := c.readerClient.Get(ctx, &v2.GetRequest{
		Key: []byte(key),
	})

	if err != nil {
		cErr := status.Convert(err)
		fmt.Println(cErr, "cErr")
		return nil, errors.New(cErr.Message())
	}

	var chunks []splitter.Chunk

	for {
		msg, err := response.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				break
			}

			if errors.Is(err, services.ErrKeyNotFound) {
				return nil, ErrKeyNotFound
			}
			cErr := status.Convert(err)
			return nil, errors.New(cErr.Message())
		}

		chunks = append(chunks, splitter.Chunk{
			Data: msg.Data,
		})
	}

	return splitter.AssembleChunks(chunks), nil
}
