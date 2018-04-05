package store

import "testing"

var data = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."

func TestGzipReadWrite(t *testing.T) {
	var comp *Gzip = NewGzip()
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
	var comp *Gzip = NewGzip()
	//defer comp.Close()
	for i := 0; i < b.N; i++ {
		_, err := comp.Encode([]byte(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGzipDecode(b *testing.B) {
	var comp *Gzip = NewGzip()
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
	var comp *Lz4 = NewLz4()
	//defer comp.Close()
	for i := 0; i < b.N; i++ {
		_, err := comp.Encode([]byte(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLz4Decode(b *testing.B) {
	var comp *Lz4 = NewLz4()
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
