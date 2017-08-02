package mrT

import (
	"fmt"
	"os"
	"testing"

	"github.com/missionMeteora/journaler"
)

const (
	testInvalidActionsFmt = "invalid number of actions, expected %d and received %d"
)

func TestMrT(t *testing.T) {
	var (
		m        *MrT
		firstTxn string
		err      error
	)

	if m, err = New("./testing/", "testing"); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./testing/")

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

	if firstTxn, err = m.peekFirstTxn(); err != nil {
		t.Fatal(err)
	}

	if err = testForEach(m, "", 3); err != nil {
		t.Fatal(err)
	}

	if err = testForEachTxn(m, "", 2); err != nil {
		t.Fatal(err)
	}

	if err = testForEach(m, firstTxn, 1); err != nil {
		t.Fatal(err)
	}

	m.Archive(func(txn *Txn) (err error) {
		txn.Put([]byte("greeting"), []byte("hello"))
		txn.Put([]byte("name"), []byte("John Doe"))
		return
	})

	if err = testForEach(m, firstTxn, 1); err != nil {
		t.Fatal(err)
	}

	if err = testForEachTxn(m, "", 2); err != nil {
		t.Fatal(err)
	}

	if err = m.Txn(func(txn *Txn) (err error) {
		txn.Put([]byte("name"), []byte("derp"))
		return
	}); err != nil {
		return
	}

	if err = testForEach(m, "", 4); err != nil {
		t.Fatal(err)
	}

	if err = testForEachTxn(m, "", 3); err != nil {
		t.Fatal(err)
	}

	m.Archive(func(txn *Txn) (err error) {
		txn.Put([]byte("greeting"), []byte("hello"))
		txn.Put([]byte("name"), []byte("derp"))
		return
	})

	if err = testForEach(m, "", 4); err != nil {
		t.Fatal(err)
	}

	if err = testForEachTxn(m, "", 3); err != nil {
		t.Fatal(err)
	}

	journaler.Success("Done!")
	return
}

func testForEach(m *MrT, start string, n int) (err error) {
	var entryCount int
	if err = m.ForEach(start, func(lineType byte, key []byte, value []byte) (err error) {
		entryCount++
		return
	}); err != nil {
		return
	}

	if entryCount != n {
		return fmt.Errorf("invalid number of entries, expected %d and recieved %d", n, entryCount)
	}

	return
}

func testForEachTxn(m *MrT, start string, n int) (err error) {
	var entryCount int
	if err = m.ForEachTxn(start, func(ti *TxnInfo) (err error) {
		switch entryCount {
		case 0:
			if len(ti.Actions) != 2 {
				return fmt.Errorf(testInvalidActionsFmt, 2, len(ti.Actions))
			}

		case 1:
			if len(ti.Actions) != 1 {
				return fmt.Errorf(testInvalidActionsFmt, 1, len(ti.Actions))
			}

		case 2:
			if len(ti.Actions) != 1 {
				return fmt.Errorf(testInvalidActionsFmt, 1, len(ti.Actions))
			}

		default:
			return fmt.Errorf("invalid number of entries, recieved %d", entryCount)
		}

		entryCount++
		return
	}); err != nil {
		return
	}

	if entryCount != n {
		return fmt.Errorf("invalid number of entries, expected %d and recieved %d", n, entryCount)
	}

	return
}
