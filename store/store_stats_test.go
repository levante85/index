package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestSHeaderSize(t *testing.T) {
	h := SHeader{}
	if int(unsafe.Sizeof(h)) != 48 {
		t.Fatal("Header size is not 40 bytes")
	}
}

func TestSHeaderUpdate(t *testing.T) {
	store := &FileBackend{
		name:    "index.",
		size:    FileSizeIdx,
		maxSize: 16 * FileSizeIdx,
	}
	store.Open()

	h := NewHeaderManager(store)
	if err := h.UpdateHeader(); err != nil {
		t.Fatal(err)
	}

	ss, err := h.Stats()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, ss.SpaceInUse, HeaderSize, "space in use should be 184")
	assert.Equal(t, ss.NumberOfEntries, 0, "numer should be 0")

	os.Remove(store.name)
}

func BenchmarkEncodingGob(b *testing.B) {
	buff := &bytes.Buffer{}
	h := SHeader{}
	g := gob.NewEncoder(buff)
	for i := 0; i < b.N; i++ {
		g.Encode(h)
	}
}

func BenchmarkEncodingBinary(b *testing.B) {
	buff := &bytes.Buffer{}
	h := SHeader{}
	for i := 0; i < b.N; i++ {
		err := binary.Write(buff, binary.LittleEndian, &h)
		if err != nil {
			b.Log(err)
		}
	}
}
