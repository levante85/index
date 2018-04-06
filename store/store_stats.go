package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"

	"github.com/levante85/index/store"
)

// Store version and magic
const (
	Magic       string = "Savatar tsdb"
	Major       uint8  = 1
	Minor       uint8  = 0
	StatusOk    uint16 = 0
	StatusDirty uint16 = 1
)

//SHeader  is the structure with the statistics from the Store
type SHeader struct {
	Magic           [16]byte
	VersionMajor    uint8
	VersionMinor    uint8
	StatusOk        uint16
	NumberOfEntries uint
	RecordSize      uint
	LastUpdated     int64
}

// SStats is the stuctures the hold the computed store statistics
type SStats struct {
	SpaceInUse      int
	SpaceLeft       int
	NumberOfEntries int
	RecordSize      uint
	LastUpdated     string
}

func newHeader() *SHeader {
	h := &SHeader{
		VersionMajor: Major,
		VersionMinor: Minor,
		StatusOk:     StatusOk,
		RecordSize:   uint(unsafe.Sizeof(SRecord{})),
		LastUpdated:  0,
	}
	copy(h.Magic[:], Magic)

	return h
}

func (h *SHeader) calculateUsageStats(fstore *FileStore) *SStats {
	dateLastUpdated := time.Unix(h.LastUpdated, 0)

	return &SStats{
		SpaceInUse:      fstore.current,
		SpaceLeft:       fstore.fileStoreMaxsize - fstore.current,
		NumberOfEntries: (fstore.current / int(h.RecordSize)) - int(unsafe.Sizeof(*h)),
		RecordSize:      h.RecordSize,
		LastUpdated:     fmt.Sprintf("%v", dateLastUpdated),
	}
}

//SHeaderManager ...
type SHeaderManager struct {
	header *SHeader
	rBuff  *bytes.Buffer
	wBuff  *bytes.Buffer
	store  *store.FileStore
}

// WriteHeader durably writes the header to the store
func (h *SHeaderManager) WriteHeader(fstore *FileStore) error {
	defer h.wBuff.Reset()

	err := binary.Write(h.wBuff, binary.LittleEndian, *h.header)
	if err != nil {
		return err
	}

	if _, err := h.store.WriteAt(h.wBuff.Bytes(), 0); err != nil {
		return err
	}

	if err := fstore.Sync(0, 40); err != nil {
		return err
	}

	return nil
}

// ReadHeader reads the header from the store
func (h *SHeaderManager) ReadHeader(fstore *FileStore) error {
	defer h.rBuff.Reset()

	buff := h.rBuff.Bytes()
	if _, err := h.store.ReadAt(buff, 0); err != nil {
		return err
	}

	h.rBuff = bytes.NewBuffer(buff)
	err := binary.Read(h.rBuff, binary.LittleEndian, *h.header)
	if err != nil {
		return err
	}

	return nil
}

// Stats returns a newly version of stats meaning reads the header each time
func (h *SHeaderManager) Stats() (*SInfo, error) {
	if err := h.ReadHeader(h.store); err != nil {
		return nil, err
	}

	return h.header.calculateUsageStats(h.store), nil
}
