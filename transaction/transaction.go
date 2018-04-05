package transaction

import (
	"bytes"
	"fmt"
	"time"

	"github.com/levante85/index/store"
)

// Tx is the transaction structure
type Tx struct {
	fname    string
	buffer   *bytes.Buffer
	fstore   *store.FileStore
	statusOk bool
}

// New creates a new transation
func New() *Tx {
	return &Tx{
		fmt.Sprintf("%v", time.Now().Unix()),
		&bytes.Buffer{},
		nil,
		true,
	}
}

// Start ends the transaction
func (t *Tx) Start() error {
	return nil
}

// Stop ends the transaction
func (t *Tx) Stop() error {
	return nil
}
