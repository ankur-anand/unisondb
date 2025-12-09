package raftwalfs

// varintSize returns the number of bytes needed to encode v as a varint.
func varintSize(v uint64) int {
	size := 1
	for v >= 0x80 {
		v >>= 7
		size++
	}
	return size
}
