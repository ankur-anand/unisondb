package raftmux

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteHeader(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		want      []byte
		wantErr   error
	}{
		{
			name:      "simple namespace",
			namespace: "orders",
			want:      []byte{0x01, 0x06, 'o', 'r', 'd', 'e', 'r', 's'},
		},
		{
			name:      "single char namespace",
			namespace: "x",
			want:      []byte{0x01, 0x01, 'x'},
		},
		{
			name:      "max length namespace",
			namespace: strings.Repeat("a", 255),
			want:      append([]byte{0x01, 0xff}, []byte(strings.Repeat("a", 255))...),
		},
		{
			name:      "empty namespace",
			namespace: "",
			wantErr:   ErrNamespaceEmpty,
		},
		{
			name:      "namespace too long",
			namespace: strings.Repeat("a", 256),
			wantErr:   ErrNamespaceTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteHeader(&buf, tt.namespace)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, buf.Bytes())
		})
	}
}

func TestReadHeader(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    string
		wantErr error
	}{
		{
			name:  "simple namespace",
			input: []byte{0x01, 0x06, 'o', 'r', 'd', 'e', 'r', 's'},
			want:  "orders",
		},
		{
			name:  "single char namespace",
			input: []byte{0x01, 0x01, 'x'},
			want:  "x",
		},
		{
			name:  "max length namespace",
			input: append([]byte{0x01, 0xff}, []byte(strings.Repeat("a", 255))...),
			want:  strings.Repeat("a", 255),
		},
		{
			name:    "unknown version",
			input:   []byte{0x02, 0x06, 'o', 'r', 'd', 'e', 'r', 's'},
			wantErr: ErrUnknownVersion,
		},
		{
			name:    "zero length namespace",
			input:   []byte{0x01, 0x00},
			wantErr: ErrNamespaceEmpty,
		},
		{
			name:    "empty input",
			input:   []byte{},
			wantErr: io.EOF,
		},
		{
			name:    "partial header",
			input:   []byte{0x01},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "truncated namespace",
			input:   []byte{0x01, 0x06, 'o', 'r', 'd'},
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.input)
			ns, err := ReadHeader(r)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, ns)
		})
	}
}

func TestWriteReadRoundtrip(t *testing.T) {
	namespaces := []string{
		"orders",
		"users",
		"events",
		"x",
		strings.Repeat("a", 255),
		"namespace-with-dashes",
		"namespace_with_underscores",
		"namespace.with.dots",
	}

	for _, ns := range namespaces {
		t.Run(ns[:min(len(ns), 20)], func(t *testing.T) {
			var buf bytes.Buffer

			err := WriteHeader(&buf, ns)
			require.NoError(t, err)

			got, err := ReadHeader(&buf)
			require.NoError(t, err)
			assert.Equal(t, ns, got)
		})
	}
}

func TestReadHeaderWithTrailingData(t *testing.T) {
	input := []byte{0x01, 0x06, 'o', 'r', 'd', 'e', 'r', 's', 'e', 'x', 't', 'r', 'a'}
	r := bytes.NewReader(input)

	ns, err := ReadHeader(r)
	require.NoError(t, err)
	assert.Equal(t, "orders", ns)

	remaining := make([]byte, 5)
	n, err := r.Read(remaining)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("extra"), remaining)
}

func TestMultipleHeadersInStream(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, WriteHeader(&buf, "first"))
	require.NoError(t, WriteHeader(&buf, "second"))
	require.NoError(t, WriteHeader(&buf, "third"))

	ns1, err := ReadHeader(&buf)
	require.NoError(t, err)
	assert.Equal(t, "first", ns1)

	ns2, err := ReadHeader(&buf)
	require.NoError(t, err)
	assert.Equal(t, "second", ns2)

	ns3, err := ReadHeader(&buf)
	require.NoError(t, err)
	assert.Equal(t, "third", ns3)

	_, err = ReadHeader(&buf)
	require.ErrorIs(t, err, io.EOF)
}
