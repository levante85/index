package transaction

import (
	"bytes"
	"fmt"
	"time"
)

// Tx is the transaction structure
type Tx struct {
	fname    string
	buffer   *bytes.Buffer
	fstore   *store.FStore
	statusOk bool
}

// New creates a new transation
func New() *Tx {
	return &Tx{
		fmt.Sprintf("%v", time.Now().Unix()),
		&bytes.Buffer{},
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
