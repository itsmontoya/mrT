package mrT

// Txn is a transaction for persistence actions
type Txn struct {
	// write func
	writeLine func(lineType byte, key, value []byte)
}

func (t *Txn) clear() {
	// Remove the fn reference to avoid any race conditions
	// If you hold onto a transaction after the function is over, there is a special place in hell for you.
	t.writeLine = nil
}

// Put will set a value
func (t *Txn) Put(key, value []byte) {
	t.writeLine(PutLine, key, value)
}

// Delete will remove a value
func (t *Txn) Delete(key []byte) {
	t.writeLine(DeleteLine, key, nil)
}
