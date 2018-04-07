package store

import (
	"bytes"
	"encoding/gob"
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
	RecordSize      int
	LastUpdated     int64
}

// SStats is the stuctures the hold the computed store statistics
type SStats struct {
	SpaceInUse      int
	SpaceLeft       int
	NumberOfEntries int
	RecordSize      int
	LastUpdated     string
}

func newHeader() *SHeader {
	h := &SHeader{
		VersionMajor: Major,
		VersionMinor: Minor,
		StatusOk:     StatusOk,
		RecordSize:   int(unsafe.Sizeof(SRecord{})),
		LastUpdated:  0,
	}
	copy(h.Magic[:], Magic)

	return h
}

func (h *SHeader) calculateUsageStats(s *FileBackend) *SStats {
	sstas := &SStats{
		SpaceInUse:      s.currPos,
		SpaceLeft:       s.maxSize - s.currPos,
		NumberOfEntries: 0,
		RecordSize:      h.RecordSize,
		LastUpdated:     fmt.Sprintf("%v", time.Unix(h.LastUpdated, 0)),
	}

	if s.currPos > 0 && h.RecordSize > 0 {
		sstas.NumberOfEntries = s.currPos - int(unsafe.Sizeof(*h))/h.RecordSize
	}

	return sstas
}

func (h *SHeader) lastUpdated() {
	h.LastUpdated = time.Now().Unix()
}

//SHeaderManager ...
type SHeaderManager struct {
	header  *SHeader
	rBuff   *bytes.Buffer
	wBuff   *bytes.Buffer
	encoder *gob.Encoder
	decoder *gob.Decoder
	store   *FileBackend
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

	if h.encoder == nil {
		h.encoder = gob.NewEncoder(h.wBuff)
	}

	err := h.encoder.Encode(h.header)
	if err != nil {
		return err
	}

	h.header.lastUpdated()
	_, err = h.store.WriteAt(h.wBuff.Bytes(), 0)
	if err != nil {
		return err
	}

	return h.store.Sync(0, HeaderSize)
}

// ReadHeader reads the header from the store
func (h *SHeaderManager) ReadHeader() error {
	defer h.rBuff.Reset()

	buff := make([]byte, 184)
	if _, err := h.store.ReadAt(buff, 0); err != nil {
		return err
	}

	h.rBuff = bytes.NewBuffer(buff)
	h.decoder = gob.NewDecoder(h.rBuff)

	return h.decoder.Decode(h.header)
}

// Stats returns a newly version of stats meaning reads the header each time
func (h *SHeaderManager) Stats() (*SStats, error) {
	if err := h.ReadHeader(); err != nil {
		return nil, err
	}

	return h.header.calculateUsageStats(h.store), nil
}
