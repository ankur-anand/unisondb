package middleware

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/common/helpers/templates"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type RequestID string
type Method string

// GetRequestInfo extracts namespace, request ID, and method from the context.
func GetRequestInfo(ctx context.Context) (string, RequestID, Method) {
	return GetNamespace(ctx), RequestID(GetRequestID(ctx)), Method(GetMethod(ctx))
}

func GetMethod(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-method"); len(values) > 0 {
		return values[0]
	}
	return ""
}

func GetNamespace(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-namespace"); len(values) > 0 {
		return values[0]
	}
	return ""
}

func GetRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-request-id"); len(values) > 0 {
		return values[0]
	}
	return ""
}

// maskClientPort removes the port from IP addresses.
func maskClientPort(address string) string {
	if idx := strings.LastIndex(address, ":"); idx != -1 {
		return address[:idx] // Remove port
	}
	return address
}

// getClientIP extracts and masks the client IP (removes port).
func getClientIP(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		ip := p.Addr.String()
		return maskClientPort(ip)
	}
	return "unknown"
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.String()
	}
	return s
}
