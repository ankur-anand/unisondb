package kvstore

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	v1 "github.com/ankur-anand/unisondb/proto/gen/go/kvalchemy/replicator/v1"
	"github.com/ankur-anand/unisondb/services"
	"github.com/ankur-anand/unisondb/splitter"
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
	writerClient v1.KVStoreWriteServiceClient
	readerClient v1.KVStoreReadServiceClient
}

func NewClient(gcc *grpc.ClientConn) *Client {
	return &Client{
		gcc:          gcc,
		writerClient: v1.NewKVStoreWriteServiceClient(gcc),
		readerClient: v1.NewKVStoreReadServiceClient(gcc),
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

	_, err := c.writerClient.Put(ctx, &v1.PutRequest{
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

	_, err := c.writerClient.Delete(ctx, &v1.DeleteRequest{
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

	err = stream.Send(&v1.PutStreamChunksForKeyRequest{
		RequestType: &v1.PutStreamChunksForKeyRequest_StartMarker{StartMarker: &v1.ChunkStartMarker{Key: []byte(key)}},
	})

	if err != nil {
		cErr := status.Convert(err)
		return errors.New(cErr.Message())
	}

	checksum := uint32(0)
	for _, chunk := range chunks {
		err = stream.Send(&v1.PutStreamChunksForKeyRequest{
			RequestType: &v1.PutStreamChunksForKeyRequest_Chunk{Chunk: &v1.ChunkPutValue{Value: chunk}},
		})
		checksum = crc32.Update(checksum, crc32.IEEETable, chunk)
		if err != nil {
			cErr := status.Convert(err)
			return errors.New(cErr.Message())
		}
	}

	// commit
	err = stream.Send(&v1.PutStreamChunksForKeyRequest{
		RequestType: &v1.PutStreamChunksForKeyRequest_CommitMarker{CommitMarker: &v1.ChunkCommitMarker{
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
	response, err := c.readerClient.Get(ctx, &v1.GetRequest{
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
