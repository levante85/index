package store

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFStoreOpenClose(t *testing.T) {
	fstore := &FileBackend{
		name:    "index.",
		size:    FileSizeDb,
		maxSize: FileSizeDb * 16,
	}
	assert.Nil(t, fstore.Open())

	stat, err := fstore.file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, stat.Size(), int64(fstore.size), "Size should be equal")

	test := []byte("this is a test")
	fstore.WriteAt(test, 0)
	fstore.Sync(0, 0)

	assert.Equal(t, stat.Size(), int64(fstore.size), "Size should be equal")
	assert.Nil(t, fstore.Close())
	assert.Nil(t, os.Remove(fstore.name))
}

func TestFStoreWrite(t *testing.T) {
	fstore := &FileBackend{
		name:    "index.",
		size:    FileSizeDb,
		maxSize: FileSizeDb * 16,
	}
	assert.Nil(t, fstore.Open())

	data := []byte("this is a test")
	n, err := fstore.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, n, len(data), "n and len(data) should be equal")

	out := make([]byte, len(data))
	n, err = fstore.ReadAt(out, 0)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, n, len(data), "n and len(data) should be equal")
	assert.True(t, bytes.Equal(data, out), "n and len(data) should be equal")

	assert.Nil(t, fstore.Close())
	assert.Nil(t, os.Remove(fstore.name))
}

func TestFStoreWriteMany(t *testing.T) {
	fstore := &FileBackend{
		name:    "index.",
		size:    FileSizeDb,
		maxSize: FileSizeDb * 16,
	}
	assert.Nil(t, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, len(data); i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	fstore.Sync(0, 0)

	for i, off := 0, len(data); i < 1024; i++ {
		out := make([]byte, len(data))
		n, err := fstore.ReadAt(out, off)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, n, len(data), "n and len(data) should be equal")
		assert.True(t, bytes.Equal(data, out), "n and len(data) should be equal")
		//		t.Log(i, n, len(data))
		//		t.Log(bytes.Equal(data, out))
		//		t.Log(string(data), string(out))
		//		t.Log("--------------------")

		off += len(data)
	}

	assert.Equal(t, fstore.currPos, 1024*len(data), "curreent offset size wrong")

	assert.Nil(t, fstore.Close())
	assert.Nil(t, os.Remove(fstore.name))
}

func BenchmarkRead(b *testing.B) {
	fstore := &FileBackend{
		name:    "index.",
		size:    FileSizeDb,
		maxSize: FileSizeDb * 16,
	}
	assert.Nil(b, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, len(data); i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}

		assert.Equal(b, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]byte, len(data))
		for i, off := 0, len(data); i < 1024; i++ {
			n, err := fstore.ReadAt(out, off)
			if err != nil {
				b.Fatal(err)
			}

			assert.Equal(b, n, len(data), "n and len(data) should be equal")
			assert.True(b, bytes.Equal(data, out), "n and len(data) should be equal")
			off += len(data)
		}
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.name))
}

func BenchmarkWrite(b *testing.B) {

	fstore := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}
	assert.Nil(b, fstore.Open())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := []byte("this is a test")
		for i, off := 0, len(data); i < 1024; i++ {
			n, err := fstore.WriteAt(data, off)
			if err != nil {
				b.Fatal(err)
			}

			assert.Equal(b, n, len(data), "n and len(data) should be equal")

			off += len(data)
		}
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.name))
}

func BenchmarkReadLarge(b *testing.B) {
	fstore := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}
	assert.Nil(b, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, len(data); i < 100000; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}

		assert.Equal(b, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]byte, len(data))
		for i, off := 0, len(data); i < 100000; i++ {
			n, err := fstore.ReadAt(out, off)
			if err != nil {
				b.Fatal(err)
			}

			assert.Equal(b, n, len(data), "n and len(data) should be equal")
			assert.True(b, bytes.Equal(data, out), "n and len(data) should be equal")

			off += len(data)
		}
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.name))
}

func BenchmarkWriteLarge(b *testing.B) {
	fstore := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}
	if err := fstore.Open(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := []byte("this is a test")
		for i, off := 0, len(data); i < 100000; i++ {
			n, err := fstore.WriteAt(data, off)
			if err != nil {
				b.Fatal(err)
			}

			assert.Equal(b, n, len(data), "n and len(data) should be equal")

			off += len(data)
		}
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.name))
}

func TestMStoreOpenClose(t *testing.T) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(t, fstore.Open())
	stat, err := fstore.fstore.file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, stat.Size(), int64(fstore.fstore.size), "Size should be equal")

	assert.Nil(t, fstore.Close())
	assert.Nil(t, os.Remove(fstore.fstore.name))
}

func TestMStoreWrite(t *testing.T) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(t, fstore.Open())

	data := []byte("this is a test")
	n, err := fstore.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, n, len(data), "n and len(data) should be equal")

	out := make([]byte, len(data))
	n, err = fstore.ReadAt(out, 0)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, n, len(data), "n and len(data) should be equal")
	assert.True(t, bytes.Equal(data, out), "n and len(data) should be equal")

	assert.Nil(t, fstore.Close())
	assert.Nil(t, os.Remove(fstore.fstore.name))
}

func TestMStoreWriteMany(t *testing.T) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(t, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, 0; i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	if err := fstore.Sync(0, 1024*len(data)); err != nil {
		t.Fatal(err)
	}

	for i, off := 0, 0; i < 1024; i++ {
		out := make([]byte, len(data))
		n, err := fstore.ReadAt(out, off)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, n, len(data), "n and len(data) should be equal")
		assert.True(t, bytes.Equal(data, out), "n and len(data) should be equal")

		off += len(data)
	}

	assert.Equal(t, fstore.fstore.currPos, 1024*len(data), "current off is wrong")

	assert.Nil(t, fstore.Close())
	assert.Nil(t, os.Remove(fstore.fstore.name))
}

func BenchmarkMStoreRead(b *testing.B) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(b, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, 0; i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}

		assert.Equal(b, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]byte, len(data))
		for i, off := 0, 0; i < 1024; i++ {
			n, err := fstore.ReadAt(out, off)
			if err != nil {
				b.Fatal(err)
			}

			assert.Equal(b, n, len(data), "n and len(data) should be equal")
			assert.True(b, bytes.Equal(data, out), "n and len(data) should be equal")

			off += len(data)
		}
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.fstore.name))

}

func BenchmarkMStoreWrite(b *testing.B) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(b, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, 0; i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}

		assert.Equal(b, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}
	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.fstore.name))

}

func BenchmarkMStoreReadLarge(b *testing.B) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(b, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, 0; i < 100000; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}

		assert.Equal(b, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]byte, len(data))
		for i, off := 0, 0; i < 100000; i++ {
			n, err := fstore.ReadAt(out, off)
			if err != nil {
				b.Fatal(err)
			}

			assert.Equal(b, n, len(data), "n and len(data) should be equal")
			assert.True(b, bytes.Equal(data, out), "n and len(data) should be equal")

			off += len(data)
		}
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.fstore.name))

}

func BenchmarkMStoreWriteLarge(b *testing.B) {

	store := &FileBackend{name: "index.", size: FileSizeDb, maxSize: FileSizeDb * 16}

	fstore := &MappedBackend{
		fstore: store,
		mstore: make([]byte, 0),
	}

	assert.Nil(b, fstore.Open())

	data := []byte("this is a test")
	for i, off := 0, 0; i < 100000; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}

		assert.Equal(b, n, len(data), "n and len(data) should be equal")

		off += len(data)
	}

	assert.Nil(b, fstore.Close())
	assert.Nil(b, os.Remove(fstore.fstore.name))
}
