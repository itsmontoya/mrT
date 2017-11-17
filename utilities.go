package mrT

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"

	"github.com/Path94/atoms"
	"github.com/itsmontoya/seeker"
)

// ForEachFn is used for iterating through entries
type ForEachFn func(lineType byte, key, value []byte) (err error)

// ForEachRawFn is used for iterating through raw entries
type ForEachRawFn func(line []byte) (err error)

// ForEachTxnFn is used for iterating through transactions
type ForEachTxnFn func(ti *TxnInfo) (err error)

// TxnFn is used for transactions
type TxnFn func(txn *Txn) error

// TxnInfo is information about a transaction
type TxnInfo struct {
	// Transaction id
	ID string `json:"id"`
	// Timestamp of transaction
	TS int64 `json:"ts"`
	// List of actions
	Actions []*ActionInfo `json:"actions"`
}

func newActionInfo(put bool, key, value []byte) *ActionInfo {
	var a ActionInfo
	a.Put = put
	a.Key = string(key)
	a.Value = string(value)
	return &a
}

// ActionInfo is information about an action
type ActionInfo struct {
	Put   bool   `json:"put"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func getFirstCommit(buf *bytes.Buffer) (err error) {
	if buf.Bytes()[0] == TransactionLine {
		return seeker.ErrEndEarly
	}

	return
}

// getKV will extract the key and value from a payload
func getKV(b []byte) (key, value []byte) {
	// Set index at 8 to accommodate 8 bytes for key length
	idx := uint64(8)
	blen := uint64(len(b))
	if blen < idx {
		return
	}

	// Get key length
	lv := binary.LittleEndian.Uint64(b[0:idx])
	if blen < idx+lv {
		return
	}

	key = b[idx : lv+idx]

	// Increment index past our key bytes
	idx += lv
	if blen < idx {
		return
	}

	// Get value length
	lv = binary.LittleEndian.Uint64(b[idx : idx+8])
	// Increment our index past the value length
	idx += 8
	if blen < lv+idx {
		return
	}

	// Get upper range in case we need to pack in data after the value
	value = b[idx : lv+idx]
	return
}

// getKVSafe will extract the key and value from a payload and apply copy on read
func getKVSafe(b []byte) (key, value []byte) {
	key, value = getKV(b)
	key = append([]byte{}, key...)
	value = append([]byte{}, value...)
	return
}

func getKey(b []byte) string {
	kb, _ := getKV(b)
	return string(kb)
}

func getLineType(buf *bytes.Buffer) (lineType byte, err error) {
	if lineType, err = buf.ReadByte(); err != nil {
		return
	}

	// Unread the byte we just read
	err = buf.UnreadByte()
	return
}

func endOnMatch(buf *bytes.Buffer) (err error) {
	return seeker.ErrEndEarly
}

type lbuf struct {
	mux atoms.Mux
	buf bytes.Buffer
}

func (l *lbuf) Update(fn func(*bytes.Buffer) error) (err error) {
	l.mux.Update(func() {
		err = fn(&l.buf)
		l.buf.Reset()
	})

	return
}

// peekFirstTxn will return the first transaction id within the current file
func peekFirstTxn(s *seeker.Seeker) (txnID string, err error) {
	if err = s.SeekToStart(); err != nil {
		return
	}

	return nextTxn(s)
}

func peekLastTxn(s *seeker.Seeker) (txnID string, err error) {
	// Seek to the end of the file
	if err = s.SeekToEnd(); err != nil {
		return
	}

	return prevTxn(s)
}

func prevTxn(s *seeker.Seeker) (txnID string, err error) {
	var lineType byte
	for {
		// Gets us to the beginning of the current line OR to the beginning of the previous line if
		// we are already at the beginning of a line
		if err = s.PrevLine(); err != nil {
			return
		}

		if err = s.ReadLine(func(buf *bytes.Buffer) (err error) {
			if lineType, err = buf.ReadByte(); err != nil {
				return
			}

			if lineType == TransactionLine {
				// Transaction found!
				tidb, _ := getKV(buf.Bytes())
				txnID = string(tidb)
				return
			}

			return
		}); err != nil {
			return
		}

		if len(txnID) > 0 {
			break
		}

		// Gets us to the beginning of the current line
		if err = s.PrevLine(); err != nil {
			return
		}
	}

	return
}

// nextTxn will return the next transaction id within the current file
func nextTxn(s *seeker.Seeker) (txnID string, err error) {
	if err = s.ReadLines(func(buf *bytes.Buffer) (err error) {
		var lineType byte
		if lineType, err = buf.ReadByte(); err != nil {
			return
		}

		if lineType != TransactionLine {
			return
		}

		tidb, _ := getKV(buf.Bytes())
		txnID = string(tidb)

		return seeker.ErrEndEarly
	}); err != nil {
		return
	}

	if txnID == "" {
		err = ErrNoTxn
		return
	}

	// Set cursor to the beginning of the transaction line
	s.PrevLine()
	return
}

func replayID(s *seeker.Seeker) (txnID string, err error) {
	if err = s.SeekToStart(); err != nil {
		return
	}

	if err = s.ReadLine(func(buf *bytes.Buffer) (err error) {
		var lineType byte
		if lineType, err = buf.ReadByte(); err != nil {
			return
		}

		if lineType != ReplayLine {
			return ErrNoTxn
		}

		tidb, _ := getKV(buf.Bytes())
		txnID = string(tidb)
		return
	}); err != nil {
		return
	}

	// Set cursor to the beginning of the transaction line
	s.PrevLine()
	return
}

func getTmp() (tmpF *os.File, name string, err error) {
	if tmpF, err = ioutil.TempFile("", "mrT"); err != nil {
		return
	}

	name = tmpF.Name()
	return
}

// ReadSeekCloser incorporates reader, seeker, and closer interfaces
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}
