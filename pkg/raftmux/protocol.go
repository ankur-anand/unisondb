package raftmux

import (
	"errors"
	"io"
)

const (
	ProtocolVersion = byte(0x01)
	NamespaceMaxLen = 255
)

var (
	ErrUnknownVersion   = errors.New("raftmux: unknown protocol version")
	ErrNamespaceTooLong = errors.New("raftmux: namespace exceeds max length")
	ErrNamespaceEmpty   = errors.New("raftmux: namespace cannot be empty")
)

// Format: [version:1][length:1][namespace:N].
func WriteHeader(w io.Writer, namespace string) error {
	if namespace == "" {
		return ErrNamespaceEmpty
	}
	if len(namespace) > NamespaceMaxLen {
		return ErrNamespaceTooLong
	}

	header := make([]byte, 2+len(namespace))
	header[0] = ProtocolVersion
	header[1] = byte(len(namespace))
	copy(header[2:], namespace)

	_, err := w.Write(header)
	return err
}

// ReadHeader reads the namespace header from r.
// Returns the namespace string or an error.
func ReadHeader(r io.Reader) (string, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return "", err
	}

	version := header[0]
	if version != ProtocolVersion {
		return "", ErrUnknownVersion
	}

	nsLen := int(header[1])
	if nsLen == 0 {
		return "", ErrNamespaceEmpty
	}

	ns := make([]byte, nsLen)
	if _, err := io.ReadFull(r, ns); err != nil {
		return "", err
	}

	return string(ns), nil
}
