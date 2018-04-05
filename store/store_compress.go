package store

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

// Compress is the interface compression algorithms should implement to be
// used inside the store, since the store in a fixed size store, meaning each
// record should have fixed size, the algos implementing this interface should
// make sure that the compressed size is always the same if 100 bytes in, output
// should alwasy be 10byte out
type Compress interface {
	Decode([]byte) (int, error)
	Encode([]byte) (int, error)
	Close() (error, error)
}

// Gzip compression implementation
type Gzip struct {
	bufR   *bytes.Buffer
	bufW   *bytes.Buffer
	reader *gzip.Reader
	writer *gzip.Writer
}

// NewGzip creates a new gzip implementation
func NewGzip() *Gzip {
	return &Gzip{
		&bytes.Buffer{},
		&bytes.Buffer{},
		nil,
		nil,
	}
}

// Encode does what is says
func (g *Gzip) Encode(in []byte) ([]byte, error) {
	if g.writer == nil {
		g.writer = gzip.NewWriter(g.bufW)
	}

	if g.bufW.Len() > 0 {
		g.bufW.Reset()
	}

	_, err := g.writer.Write(in)
	if err != nil {
		return nil, err
	}

	err = g.writer.Close()
	if err != nil {
		return nil, err
	}

	return g.bufW.Bytes(), nil

}

// Decode does what is says
func (g *Gzip) Decode(in []byte) ([]byte, error) {
	if g.bufR.Len() > 0 {
		g.bufR.Reset()
	}

	_, err := g.bufR.Write(in)
	if err != nil {
		return nil, err
	}

	if g.reader == nil {
		var err error
		g.reader, err = gzip.NewReader(g.bufR)
		if err != nil {
			return nil, err
		}
	}

	out, err := ioutil.ReadAll(g.reader)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Close the Gzip Compressor and the underlying reader and writer
func (g *Gzip) Close() (errR error, errW error) {
	if g.reader != nil {
		errR = g.reader.Close()
	}

	if g.writer != nil {
		errW = g.writer.Close()
	}

	return

}

// Lz4 whatevver
type Lz4 struct {
	bufR   *bytes.Buffer
	bufW   *bytes.Buffer
	writer *lz4.Writer
	reader *lz4.Reader
}

// NewLz4 creates a new lz4 implementation
func NewLz4() *Lz4 {
	return &Lz4{
		&bytes.Buffer{},
		&bytes.Buffer{},
		nil,
		nil,
	}
}

// Encode does what is says
func (g *Lz4) Encode(in []byte) ([]byte, error) {
	if g.writer == nil {
		g.writer = lz4.NewWriter(g.bufW)
	}

	if g.bufW.Len() > 0 {
		g.bufW.Reset()
	}

	_, err := g.writer.Write(in)
	if err != nil {
		return nil, err
	}

	err = g.writer.Close()
	if err != nil {
		return nil, err
	}

	return g.bufW.Bytes(), nil

}

// Decode does what is says
func (g *Lz4) Decode(in []byte) ([]byte, error) {
	if g.bufR.Len() > 0 {
		g.bufR.Reset()
	}

	_, err := g.bufR.Write(in)
	if err != nil {
		return nil, err
	}

	if g.reader == nil {
		g.reader = lz4.NewReader(g.bufR)
	}

	out, err := ioutil.ReadAll(g.reader)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Close the Gzip Compressor and the underlying reader and writer
func (g *Lz4) Close() (errW error) {
	if g.writer != nil {
		errW = g.writer.Close()
	}

	return

}

// Snappy whatevver
type Snappy struct {
	bufR   *bytes.Buffer
	bufW   *bytes.Buffer
	writer *snappy.Writer
	reader *snappy.Reader
}

// NewSnappy creates a new snappy implementation
func NewSnappy() *Snappy {
	return &Snappy{
		&bytes.Buffer{},
		&bytes.Buffer{},
		nil,
		nil,
	}
}

// Encode does what is says
func (g *Snappy) Encode(in []byte) ([]byte, error) {
	if g.writer == nil {
		g.writer = snappy.NewWriter(g.bufW)
	}

	if g.bufW.Len() > 0 {
		g.bufW.Reset()
	}

	_, err := g.writer.Write(in)
	if err != nil {
		return nil, err
	}

	err = g.writer.Flush()
	if err != nil {
		return nil, err
	}

	return g.bufW.Bytes(), nil

}

// Decode does what is says
func (g *Snappy) Decode(in []byte) ([]byte, error) {
	if g.bufR.Len() > 0 {
		g.bufR.Reset()
	}

	_, err := g.bufR.Write(in)
	if err != nil {
		return nil, err
	}

	if g.reader == nil {
		g.reader = snappy.NewReader(g.bufR)
	}

	out, err := ioutil.ReadAll(g.reader)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Close the Gzip Compressor and the underlying reader and writer
func (g *Snappy) Close() (errW error) {
	if g.writer != nil {
		errW = g.writer.Close()
	}

	return

}
