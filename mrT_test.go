package mrT

import (
	"testing"
)

func TestMrT(t *testing.T) {
	var (
		m   *MrT
		err error
	)

	if m, err = New("./testing/", "testing"); err != nil {
		t.Fatal(err)
	}

	if err = m.Txn(func(txn *Txn) (err error) {
		txn.Put([]byte("greeting"), []byte("hello"))
		txn.Put([]byte("name"), []byte("world"))
		return
	}); err != nil {
		return
	}

	if err = m.Txn(func(txn *Txn) (err error) {
		txn.Put([]byte("name"), []byte("John Doe"))
		return
	}); err != nil {
		return
	}

	var entryCount int
	m.ForEach(func(lineType byte, key []byte, value []byte) (end bool) {
		entryCount++
		return
	})

	if entryCount != 3 {
		t.Fatalf("invalid entry count, expected %d and received %d", 4, entryCount)
	}

	entryCount = 0
	m.ForEachTxn(func(ti *TxnInfo) (end bool) {
		switch entryCount {
		case 0:
			if len(ti.Actions) != 2 {
				t.Fatalf("invalid number of actions, expected %d and received %d", 2, len(ti.Actions))
			}

		case 1:
			if len(ti.Actions) != 1 {
				t.Fatalf("invalid number of actions, expected %d and received %d", 1, len(ti.Actions))
			}

		default:
			t.Fatal("invalid number of transaction entries")
			return true
		}

		entryCount++
		return
	})

	m.Archive(func(txn *Txn) (err error) {
		txn.Put([]byte("greeting"), []byte("hello"))
		txn.Put([]byte("name"), []byte("world :)"))
		return
	})

	if err = m.Txn(func(txn *Txn) (err error) {
		txn.Put([]byte("name"), []byte("derp"))
		return
	}); err != nil {
		return
	}

	m.Archive(func(txn *Txn) (err error) {
		txn.Put([]byte("greeting"), []byte("hello"))
		txn.Put([]byte("name"), []byte("derp"))
		return
	})

	return
}
