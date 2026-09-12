package walfs

// RecordDecoder transforms raw WAL bytes into the record payload, for callers
// that store records inside an outer envelope and need it stripped on read.
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
