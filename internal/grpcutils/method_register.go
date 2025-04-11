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

func RegisterGRPCSMethods(server *grpc.Server) map[string]string {
	methodTypes := make(map[string]string)

	for svc, info := range server.GetServiceInfo() {
		for _, method := range info.Methods {
			fullMethod := fmt.Sprintf("/%s/%s", svc, method.Name)

			methodType := unary
			if method.IsClientStream && method.IsServerStream {
				methodType = bidiStream
			} else if method.IsClientStream {
				methodType = clientStream
			} else if method.IsServerStream {
				methodType = serverStream
			}
			methodTypes[fullMethod] = methodType
		}
	}

	return methodTypes
}
