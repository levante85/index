package transaction

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/levante85/index/store"
)

const stampSize = 16

// Tx is the transaction structure
type Tx struct {
	name     string
	buffer   *bytes.Buffer
	store    store.Store
	statusOk bool
	checker  store.DataChecker
}

// New creates a new transation
func New() *Tx {
	tx := &Tx{
		fmt.Sprintf("%v", time.Now().Unix()),
		&bytes.Buffer{},
		nil,
		true,
		store.NewCrc64(),
	}

	tx.store = store.New(
		&store.Conf{
			Name: tx.name,
			Size: store.FileSizeTx,
			Mode: store.NORMAL,
		},
	)

	return tx
}

func (t *Tx) write(tstamp int64, off int) (err error) {
	defer t.buffer.Reset()

	var (
		start int
		stop  int
	)

	switch off {
	case 0:
		start = off
		stop = stampSize
	default:
		start = off
		stop = off + stampSize
	}

	defer t.store.Sync(start, stop)

	err = binary.Write(t.buffer, binary.LittleEndian, tstamp)
	if err != nil {
		return err
	}

	csum := t.checker.Checksum(t.buffer.Bytes())
	err = binary.Write(t.buffer, binary.LittleEndian, csum)
	if err != nil {
		return err
	}

	_, err = t.store.WriteAt(t.buffer.Bytes(), off)
	if err != nil {
		return err
	}

	return err
}

func (t *Tx) read(buff []byte, off int) error {
	// implement read need for roll back
	_, err := t.store.ReadAt(buff, off)
	if err != nil {
		return err
	}

	return nil
}

// Start ends the transaction
func (t *Tx) Start() error {
	stamp := time.Now().Unix()

	err := t.store.Open()
	if err != nil {
		return err
	}

	return t.write(stamp, 0)
}

/* Add element to the transaction log
func (t *Tx) Add() error {
	return nil
}
*/

// Stop ends the transaction
func (t *Tx) Stop() error {
	defer t.store.Close()

	stamp := time.Now().Unix()
	err := t.write(stamp, stampSize)
	if err != nil {
		return err
	}

	return nil
}
