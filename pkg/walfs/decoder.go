package walfs

// - Wrapped: WAL bytes contain a wrapper (e.g., Raft log) that must be unwrapped.
type RecordDecoder interface {
	// Decode transforms raw WAL bytes into the actual record payload.
	// The returned bytes may reference the input (zero-copy) or be newly allocated.
	Decode(data []byte) ([]byte, error)
}

type NoopDecoder struct{}

func (d NoopDecoder) Decode(data []byte) ([]byte, error) {
	return data, nil
}

var _ RecordDecoder = NoopDecoder{}
