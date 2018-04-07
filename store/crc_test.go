package store

import "testing"

func BenchmarkCrc32(b *testing.B) {
	crc := NewCrc32()
	for i := 0; i < b.N; i++ {
		crc.Checksum([]byte(data))
	}
}

func BenchmarkCrc64(b *testing.B) {
	crc := NewCrc64()
	for i := 0; i < b.N; i++ {
		crc.Checksum([]byte(data))
	}
}
