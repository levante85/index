package store

import (
	"hash/crc32"
	"hash/crc64"
)

// DataChecker is the interface that describes the crc or other implementations
// used throught the index implementation to verify data validity
type DataChecker interface {
	Checksum(data []byte) uint64
}

// CrcChecker64 implements a crc64 based data checker
type CrcChecker64 struct {
	table *crc64.Table
}

//NewCrc64 instanciate a new crc64 data checker
func NewCrc64() *CrcChecker64 {
	return &CrcChecker64{crc64.MakeTable(crc64.ISO)}
}

// Checksum is the fuction the checks the data
func (c *CrcChecker64) Checksum(data []byte) uint64 {
	return crc64.Checksum(data, c.table)
}

// CrcChecker32 implements a crc64 based data checker
type CrcChecker32 struct {
	table *crc32.Table
}

//NewCrc32 instanciate a new crc64 data checker
func NewCrc32() *CrcChecker32 {
	return &CrcChecker32{crc32.MakeTable(crc32.IEEE)}
}

// Checksum is the fuction the checks the data
func (c *CrcChecker32) Checksum(data []byte) uint32 {
	return crc32.Checksum(data, c.table)
}
