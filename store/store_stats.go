package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"
)

// Store version and magic
const (
	Magic       string = "Savatar tsdb"
	Major       uint16 = 1
	Minor       uint16 = 0
	StatusOk    uint16 = 0
	StatusDirty uint16 = 1
	HeaderSize  int    = 48
)

//SHeader  is the structure with the statistics from the Store
type SHeader struct {
	Magic           [18]byte
	VersionMajor    uint16
	VersionMinor    uint16
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

func (h *SHeader) calculateUsageStats(fstore *FileBackend) *SStats {
	dateLastUpdated := time.Unix(h.LastUpdated, 0)

	return &SStats{
		SpaceInUse:      fstore.current,
		SpaceLeft:       fstore.fileStoreMaxsize - fstore.current,
		NumberOfEntries: (fstore.current - int(unsafe.Sizeof(*h))/int(h.RecordSize)),
		RecordSize:      h.RecordSize,
		LastUpdated:     fmt.Sprintf("%v", dateLastUpdated),
	}
}

func (h *SHeader) lastUpdated() {
	h.LastUpdated = time.Now().Unix()
}

//SHeaderManager ...
type SHeaderManager struct {
	header *SHeader
	rBuff  *bytes.Buffer
	wBuff  *bytes.Buffer
	store  *FileBackend
}

// NewHeaderManager instantian a new header manager the performs operations
// on the store header such as read and update and store statistics
func NewHeaderManager(s *FileBackend) *SHeaderManager {
	return &SHeaderManager{
		header: newHeader(),
		rBuff:  &bytes.Buffer{},
		wBuff:  &bytes.Buffer{},
		store:  s,
	}
}

// UpdateHeader durably writes the header to the store
func (h *SHeaderManager) UpdateHeader() error {
	defer h.wBuff.Reset()

	err := binary.Write(h.wBuff, binary.LittleEndian, h.header)
	if err != nil {
		return err
	}

	if _, err := h.store.WriteAt(h.wBuff.Bytes(), HeaderSize); err != nil {
		return err
	}

	h.header.lastUpdated()

	return h.store.Sync(0, HeaderSize)
}

// ReadHeader reads the header from the store
func (h *SHeaderManager) ReadHeader() error {
	defer h.rBuff.Reset()

	buff := h.rBuff.Bytes()
	if _, err := h.store.ReadAt(buff, 0); err != nil {
		return err
	}

	h.rBuff = bytes.NewBuffer(buff)
	return binary.Read(h.rBuff, binary.LittleEndian, *h.header)
}

// Stats returns a newly version of stats meaning reads the header each time
func (h *SHeaderManager) Stats() (*SStats, error) {
	if err := h.ReadHeader(); err != nil {
		return nil, err
	}

	return h.header.calculateUsageStats(h.store), nil
}
