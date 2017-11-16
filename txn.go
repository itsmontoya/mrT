package mrT

import "bytes"

func newTxn(buf *bytes.Buffer, fn WriteFn) (txn Txn) {
	txn.buf = buf
	txn.writeLine = fn
	return
}

// Txn is a transaction for persistence actions
type Txn struct {
	buf *bytes.Buffer
	// write func
	writeLine WriteFn
}

func (t *Txn) clear() {
	// Clear references
	t.buf = nil
	t.writeLine = nil
	// If you hold onto a transaction after the function is over, there is a special place in hell for you.
}

// Put will set a value
func (t *Txn) Put(key, value []byte) error {
	return t.writeLine(t.buf, PutLine, key, value)
}

// Delete will remove a value
func (t *Txn) Delete(key []byte) error {
	return t.writeLine(t.buf, DeleteLine, key, nil)
}

// WriteFn is the function signature for calling mrT.writeLn
type WriteFn func(buf *bytes.Buffer, lineType byte, key, value []byte) error
