// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: unisondb/streamer/v1/streamer.proto

package v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	WalStreamerService_StreamWalRecords_FullMethodName = "/unisondb.streamer.v1.WalStreamerService/StreamWalRecords"
	WalStreamerService_GetLatestOffset_FullMethodName  = "/unisondb.streamer.v1.WalStreamerService/GetLatestOffset"
)

// WalStreamerServiceClient is the client API for WalStreamerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// WalStreamerService streams Wal Records.
type WalStreamerServiceClient interface {
	// / Stream WAL Logs
	StreamWalRecords(ctx context.Context, in *StreamWalRecordsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[StreamWalRecordsResponse], error)
	GetLatestOffset(ctx context.Context, in *GetLatestOffsetRequest, opts ...grpc.CallOption) (*GetLatestOffsetResponse, error)
}

type walStreamerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWalStreamerServiceClient(cc grpc.ClientConnInterface) WalStreamerServiceClient {
	return &walStreamerServiceClient{cc}
}

func (c *walStreamerServiceClient) StreamWalRecords(ctx context.Context, in *StreamWalRecordsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[StreamWalRecordsResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &WalStreamerService_ServiceDesc.Streams[0], WalStreamerService_StreamWalRecords_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[StreamWalRecordsRequest, StreamWalRecordsResponse]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type WalStreamerService_StreamWalRecordsClient = grpc.ServerStreamingClient[StreamWalRecordsResponse]

func (c *walStreamerServiceClient) GetLatestOffset(ctx context.Context, in *GetLatestOffsetRequest, opts ...grpc.CallOption) (*GetLatestOffsetResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetLatestOffsetResponse)
	err := c.cc.Invoke(ctx, WalStreamerService_GetLatestOffset_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WalStreamerServiceServer is the server API for WalStreamerService service.
// All implementations must embed UnimplementedWalStreamerServiceServer
// for forward compatibility.
//
// WalStreamerService streams Wal Records.
type WalStreamerServiceServer interface {
	// / Stream WAL Logs
	StreamWalRecords(*StreamWalRecordsRequest, grpc.ServerStreamingServer[StreamWalRecordsResponse]) error
	GetLatestOffset(context.Context, *GetLatestOffsetRequest) (*GetLatestOffsetResponse, error)
	mustEmbedUnimplementedWalStreamerServiceServer()
}

// UnimplementedWalStreamerServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedWalStreamerServiceServer struct{}

func (UnimplementedWalStreamerServiceServer) StreamWalRecords(*StreamWalRecordsRequest, grpc.ServerStreamingServer[StreamWalRecordsResponse]) error {
	return status.Errorf(codes.Unimplemented, "method StreamWalRecords not implemented")
}
func (UnimplementedWalStreamerServiceServer) GetLatestOffset(context.Context, *GetLatestOffsetRequest) (*GetLatestOffsetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLatestOffset not implemented")
}
func (UnimplementedWalStreamerServiceServer) mustEmbedUnimplementedWalStreamerServiceServer() {}
func (UnimplementedWalStreamerServiceServer) testEmbeddedByValue()                            {}

// UnsafeWalStreamerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WalStreamerServiceServer will
// result in compilation errors.
type UnsafeWalStreamerServiceServer interface {
	mustEmbedUnimplementedWalStreamerServiceServer()
}

func RegisterWalStreamerServiceServer(s grpc.ServiceRegistrar, srv WalStreamerServiceServer) {
	// If the following call pancis, it indicates UnimplementedWalStreamerServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&WalStreamerService_ServiceDesc, srv)
}

func _WalStreamerService_StreamWalRecords_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StreamWalRecordsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WalStreamerServiceServer).StreamWalRecords(m, &grpc.GenericServerStream[StreamWalRecordsRequest, StreamWalRecordsResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type WalStreamerService_StreamWalRecordsServer = grpc.ServerStreamingServer[StreamWalRecordsResponse]

func _WalStreamerService_GetLatestOffset_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetLatestOffsetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WalStreamerServiceServer).GetLatestOffset(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WalStreamerService_GetLatestOffset_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WalStreamerServiceServer).GetLatestOffset(ctx, req.(*GetLatestOffsetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// WalStreamerService_ServiceDesc is the grpc.ServiceDesc for WalStreamerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var WalStreamerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "unisondb.streamer.v1.WalStreamerService",
	HandlerType: (*WalStreamerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetLatestOffset",
			Handler:    _WalStreamerService_GetLatestOffset_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamWalRecords",
			Handler:       _WalStreamerService_StreamWalRecords_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "unisondb/streamer/v1/streamer.proto",
}
