package grpcutils

import (
	"fmt"

	"google.golang.org/grpc"
)

const (
	bidiStream   = "bidi_stream"
	serverStream = "server_stream"
	clientStream = "client_stream"
	unary        = "unary"
)

func RegisterGRPCSMethods(services ...grpc.ServiceDesc) map[string]string {
	methodTypes := make(map[string]string)
	for _, svc := range services {
		for _, m := range svc.Methods {
			fullMethod := fmt.Sprintf("/%s/%s", svc.ServiceName, m.MethodName)
			methodTypes[fullMethod] = unary
		}
		for _, s := range svc.Streams {
			fullMethod := fmt.Sprintf("/%s/%s", svc.ServiceName, s.StreamName)

			methodType := serverStream
			if s.ClientStreams && s.ServerStreams {
				methodType = bidiStream
			} else if s.ClientStreams {
				methodType = clientStream
			} else if s.ServerStreams {
				methodType = serverStream
			}
			methodTypes[fullMethod] = methodType
		}
	}
	return methodTypes
}
