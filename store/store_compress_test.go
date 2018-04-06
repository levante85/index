package store

import (
	"bytes"
	"encoding/binary"
	"testing"
)

var data = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."

func TestGzipReadWrite(t *testing.T) {
	comp := NewGzip()
	//defer comp.Close()
	encoded, err := comp.Encode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := comp.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if string(decoded) != data {
		t.Fatal("Failed origin and decompressed data differ!")
	}
}

func BenchmarkGzipEncode(b *testing.B) {
	comp := NewGzip()
	//defer comp.Close()
	for i := 0; i < b.N; i++ {
		_, err := comp.Encode([]byte(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGzipDecode(b *testing.B) {
	comp := NewGzip()
	//defer comp.Close()
	encoded, err := comp.Encode([]byte(data))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := comp.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}

}

func TestLz4ReadWrite(t *testing.T) {
	comp := NewLz4()
	//defer comp.Close()
	encoded, err := comp.Encode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := comp.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if string(decoded) != data {
		t.Fatal("Failed origin and decompressed data differ!")
	}
}

func BenchmarkLz4Encode(b *testing.B) {
	comp := NewLz4()
	//defer comp.Close()
	for i := 0; i < b.N; i++ {
		_, err := comp.Encode([]byte(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLz4Decode(b *testing.B) {
	comp := NewLz4()
	//defer comp.Close()
	encoded, err := comp.Encode([]byte(data))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := comp.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}

}

func TestSnappyReadWrite(t *testing.T) {
	comp := NewSnappy()
	//defer comp.Close()
	encoded, err := comp.Encode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := comp.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if string(decoded) != data {
		t.Fatal("Failed origin and decompressed data differ!")
	}
}

func TestSnappyCompare(t *testing.T) {
	var i uint64 = 10
	var j uint64 = 10000000
	var z uint64 = 10000000

	bi := make([]byte, 8)
	bj := make([]byte, 8)
	bz := make([]byte, 8)

	comp := NewSnappy()

	binary.LittleEndian.PutUint64(bi, i)
	_, err := comp.Encode(bi)
	if err != nil {
		t.Fatal(err)
	}

	binary.LittleEndian.PutUint64(bj, j)
	large, err := comp.Encode(bj)
	if err != nil {
		t.Fatal(err)
	}

	binary.LittleEndian.PutUint64(bz, z)
	equal, err := comp.Encode(bz)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(equal, large) {
		t.Log("Uint once compressed are not equal")
		t.Logf("Size of equal is %v anad large is %v\n", len(equal), len(large))
	}

	for i := 0; i < 1000000; i++ {
		binary.LittleEndian.PutUint64(bi, uint64(i))
		small, err := comp.Encode(bi)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(small, large) < 0 {
			t.Log("Uint once compressed do not keep comparing the same")
		}
	}

}

func TestSnappyEncodeUints(t *testing.T) {
	comp := NewSnappy()
	buff := &bytes.Buffer{}
	for curr, i := 0, 0; i <= 31; i, curr = i+1, curr+8 {
		b := make([]byte, 8)
		var num uint64
		binary.LittleEndian.PutUint64(b, num)
		buff.Write(b)
	}

	output, err := comp.Encode(buff.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Output size is %v and input size is %v\n", len(output), buff.Len())
}

func BenchmarkSnappyEncode(b *testing.B) {
	comp := NewSnappy()
	for i := 0; i < b.N; i++ {
		_, err := comp.Encode([]byte(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnappyDecode(b *testing.B) {
	comp := NewSnappy()
	encoded, err := comp.Encode([]byte(data))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := comp.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}

}
