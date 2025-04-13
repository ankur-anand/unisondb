package svcutils

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// NewTLSCreds creates transport credentials for server-auth TLS.
// nolint:ireturn
func NewTLSCreds(caPath string) (credentials.TransportCredentials, error) {
	creds, err := credentials.NewClientTLSFromFile(caPath, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS credentials: %w", err)
	}
	return creds, nil
}

// NewMTLSCreds creates transport credentials for mutual TLS.
// nolint:ireturn
func NewMTLSCreds(certPath, keyPath, caPath string) (credentials.TransportCredentials, error) {
	clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert/key: %w", err)
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}
	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(caCert); !ok {
		return nil, errors.New("failed to append CA cert to pool")
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
	}

	return credentials.NewTLS(tlsCfg), nil
}
