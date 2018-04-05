package store

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestFStoreOpenClose(t *testing.T) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
		t.Fatal(err)
	}

	stat, err := fstore.files[0].Stat()
	if err != nil {
		t.Fatal(err)
	}

	if size := stat.Size(); size != FileSize {
		t.Fatalf("Size shuold be %v and instead is %v\n", FileSize, size)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	_, err = os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}

	err = fstore.Close()
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(name)
}

func TestFStoreWrite(t *testing.T) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
		t.Fatal(err)
	}

	stat, err := fstore.files[0].Stat()
	if err != nil {
		t.Fatal(err)
	}

	if size := stat.Size(); size != FileSize {
		t.Fatalf("Size shuold be %v and instead is %v\n", FileSize, size)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	_, err = os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("this is a test")
	n, err := fstore.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatal("n and len(data) should be equal")
	}

	out := make([]byte, len(data))
	n, err = fstore.ReadAt(out, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatal("n and len(data) should be equal")
	}

	if bytes.Equal(data, out) != true {
		t.Fatal("data should be equal")
	}

	err = fstore.Close()
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(name)

}

func TestFStoreWriteMany(t *testing.T) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
		t.Fatal(err)
	}

	stat, err := fstore.files[0].Stat()
	if err != nil {
		t.Fatal(err)
	}

	if size := stat.Size(); size != FileSize {
		t.Fatalf("Size shuold be %v and instead is %v\n", FileSize, size)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	_, err = os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("this is a test")
	for i, off := 0, len(data); i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(data) {
			t.Fatal("n and len(data) should be equal")
		}

		off += len(data)
	}

	for i, off := 0, len(data); i < 1024; i++ {
		out := make([]byte, len(data))
		n, err := fstore.ReadAt(out, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(data) {
			t.Fatal("n and len(data) should be equal")
		}

		if bytes.Equal(data, out) != true {
			t.Fatal("data should be equal")
		}
		off += len(data)
	}

	if int(fstore.current) != 1024*len(data) {
		t.Fatal("current offset is in wrong: ", fstore.current, 1024*len(data))
	}

	err = fstore.Close()
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(name)

}

func BenchmarkRead(b *testing.B) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
		b.Fatal(err)
	}

	data := []byte("this is a test")
	for i, off := 0, len(data); i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(data) {
			b.Fatal("n and len(data) should be equal")
		}

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
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			if bytes.Equal(data, out) != true {
				b.Fatal("data should be equal")
			}
			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	os.Remove(name)

}

func BenchmarkWrite(b *testing.B) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := []byte("this is a test")
		for i, off := 0, len(data); i < 1024; i++ {
			n, err := fstore.WriteAt(data, off)
			if err != nil {
				b.Fatal(err)
			}
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	os.Remove(name)

}

func BenchmarkReadLarge(b *testing.B) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
		b.Fatal(err)
	}

	data := []byte("this is a test")
	for i, off := 0, len(data); i < 100000; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(data) {
			b.Fatal("n and len(data) should be equal")
		}

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
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			if bytes.Equal(data, out) != true {
				b.Fatal("data should be equal")
			}
			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	os.Remove(name)

}

func BenchmarkWriteLarge(b *testing.B) {
	fstore := newFileStore()
	if err := fstore.Create(); err != nil {
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
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fname)
	os.Remove(name)

}

func TestMStoreOpenClose(t *testing.T) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
		t.Fatal(err)
	}

	stat, err := fstore.fstore.files[0].Stat()
	if err != nil {
		t.Fatal(err)
	}

	if size := stat.Size(); size != FileSize {
		t.Fatalf("Size shuold be %v and instead is %v\n", FileSize, size)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)
	_, err = os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}

	err = fstore.Close()
	if err != nil {
		t.Fatal("Close: ", err)
	}

	os.Remove(name)
}

func TestMStoreWrite(t *testing.T) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)

	data := []byte("this is a test")
	n, err := fstore.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatal("n and len(data) should be equal")
	}

	out := make([]byte, len(data))
	n, err = fstore.ReadAt(out, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatal("n and len(data) should be equal")
	}

	if bytes.Equal(data, out) != true {
		t.Fatal("data should be equal: ", string(data), string(out))
	}

	err = fstore.Close()
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(name)

}

func TestMStoreWriteMany(t *testing.T) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)

	data := []byte("this is a test")
	for i, off := 0, 0; i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(data) {
			t.Fatal("n and len(data) should be equal")
		}

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
		if n != len(data) {
			t.Fatal("n and len(data) should be equal")
		}

		if bytes.Equal(data, out) != true {
			t.Fatal("data should be equal at inter: ", i, string(data), string(out))
		}

		off += len(data)
	}

	if int(fstore.fstore.current) != 1024*len(data) {
		t.Fatal("current offset is in wrong: ", fstore.fstore.current, 1024*len(data))
	}

	err := fstore.Close()
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(name)

}

func BenchmarkMStoreRead(b *testing.B) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
		b.Fatal(err)
	}

	data := []byte("this is a test")
	for i, off := 0, 0; i < 1024; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(data) {
			b.Fatal("n and len(data) should be equal")
		}

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
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			if bytes.Equal(data, out) != true {
				b.Fatal("data should be equal")
			}
			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)
	os.Remove(name)

}

func BenchmarkMStoreWrite(b *testing.B) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := []byte("this is a test")
		for i, off := 0, 0; i < 1024; i++ {
			n, err := fstore.WriteAt(data, off)
			if err != nil {
				b.Fatal(err)
			}
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)
	os.Remove(name)

}

func BenchmarkMStoreReadLarge(b *testing.B) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
		b.Fatal(err)
	}

	data := []byte("this is a test")
	for i, off := 0, 0; i < 100000; i++ {
		n, err := fstore.WriteAt(data, off)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(data) {
			b.Fatal("n and len(data) should be equal")
		}

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
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			if bytes.Equal(data, out) != true {
				b.Fatal("data should be equal")
			}
			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)
	os.Remove(name)

}

func BenchmarkMStoreWriteLarge(b *testing.B) {
	fstore := newMappedStore()
	if err := fstore.Create(); err != nil {
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
			if n != len(data) {
				b.Fatal("n and len(data) should be equal")
			}

			off += len(data)
		}
	}

	err := fstore.Close()
	if err != nil {
		b.Fatal(err)
	}

	name := fmt.Sprintf("%v0", fstore.fstore.fname)
	os.Remove(name)

}
