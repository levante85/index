package store

import (
	"os"
	"testing"
	"unsafe"
)

func TestSHeaderSize(t *testing.T) {
	h := SHeader{}
	if int(unsafe.Sizeof(h)) != 48 {
		t.Fatal("Header size is not 40 bytes")
	}
}

func TestSHeaderUpdate(t *testing.T) {
	store := &FileBackend{
		fname:            "index.",
		fileStoreMaxsize: 16 * FileSizeIdx,
	}

	h := NewHeaderManager(store)
	if err := h.UpdateHeader(); err != nil {
		t.Fatal(err)
	}

	ss, err := h.Stats()
	if err != nil {
		t.Fatal(err)
	}

	if ss.SpaceInUse != 48 {
		t.Fatal("Space in use should be 48 after updating the header")
	}

	if ss.NumberOfEntries != 0 {
		t.Fatal("Number of entries should be 0 after updating the header")
	}

	os.Remove(store.fname)
}
