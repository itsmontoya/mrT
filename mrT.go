package mrT

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/itsmontoya/async/file"
	"github.com/itsmontoya/middleware"
	"github.com/itsmontoya/seeker"
	"github.com/missionMeteora/journaler"
	"github.com/missionMeteora/toolkit/errors"
	"github.com/missionMeteora/uuid"
)

const (
	// NilLine represents a zero-value for line types
	NilLine byte = iota
	// TransactionLine is the line with the transaction tag for the data lines following it
	TransactionLine
	// CommentLine is a comment line, will be ignored when parsing data
	// Note: This line type will always ignore middleware
	CommentLine
	// PutLine is for setting data
	PutLine
	// DeleteLine is for removing data
	DeleteLine
)

const (
	// ErrInvalidLine is returned when an invalid line is encountered while parsing
	ErrInvalidLine = errors.Error("invalid line")
)

var (
	newlineBytes = []byte{'\n'}
)

// New will return a new instance of MrT
func New(dir, name string, mws ...middleware.Middleware) (mp *MrT, err error) {
	var mrT MrT
	// Make the dirs needed for file
	if err = os.MkdirAll(path.Join(dir, "archive"), 0755); err != nil {
		return
	}

	if mrT.f, err = file.OpenFile(path.Join(dir, name+".tdb"), os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return
	}

	mrT.dir = dir
	mrT.name = name

	// Create new uuid generator
	mrT.ug = uuid.NewGen()
	// Create new write buffer
	mrT.buf = bytes.NewBuffer(nil)
	// Create new seeker
	mrT.s = seeker.New(mrT.f)
	// Set Mr.T's middleware
	mrT.setMWs(mws)

	if err = mrT.s.SeekToEnd(); err != nil {
		return
	}

	mp = &mrT
	return
}

// MrT is Mister Transaction, he manages file transactions
// He also pities a fool
type MrT struct {
	mux sync.RWMutex

	dir  string
	name string
	// Copy on read
	cor bool

	f  *file.File
	s  *seeker.Seeker
	ug *uuid.Gen
	mw *middleware.MWs

	buf  *bytes.Buffer
	nbuf [8]byte

	closed bool
}

func (m *MrT) setMWs(mws []middleware.Middleware) {
	if len(mws) == 0 {
		return
	}

	m.mw = middleware.NewMWs(mws...)
	return
}

func (m *MrT) isMWWrite(lineType byte) bool {
	if m.mw == nil {
		return false
	}

	return lineType == PutLine || lineType == DeleteLine
}

func (m *MrT) writeLine(lineType byte, key, value []byte) (err error) {
	// Write line type
	m.buf.WriteByte(lineType)

	// If this is not a middleware write, use fast-path
	if !m.isMWWrite(lineType) {
		m.writeRawBytes(key, value)
	} else {
		if err = m.writeMWBytes(key, value); err != nil {
			return
		}
	}

	m.buf.WriteByte('\n')
	return
}

func (m *MrT) writeRawBytes(key, value []byte) (err error) {
	// We don't check for errors because only middleware can cause errors
	m.writeBytes(m.buf, key)
	m.writeBytes(m.buf, value)
	return
}

func (m *MrT) writeMWBytes(key, value []byte) (err error) {
	var w *middleware.Writer
	if w, err = m.mw.Writer(m.buf); err != nil {
		return
	}
	defer w.Close()

	if err = m.writeBytes(w, key); err != nil {
		return
	}

	if err = m.writeBytes(w, value); err != nil {
		return
	}

	return
}

func (m *MrT) writeBytes(w io.Writer, b []byte) (err error) {
	binary.LittleEndian.PutUint64(m.nbuf[:], uint64(len(b)))
	if _, err = w.Write(m.nbuf[:]); err != nil {
		return
	}

	if _, err = w.Write(b); err != nil {
		return
	}

	return
}

func (m *MrT) flush() (err error) {
	// Ensure we are at the end before flushing
	if err = m.s.SeekToEnd(); err != nil {
		return
	}

	_, err = io.Copy(m.f, m.buf)
	m.buf.Reset()
	return
}

func (m *MrT) rollback() {
	m.buf.Reset()
}

func (m *MrT) newCommitID() (commitID []byte) {
	return []byte(m.ug.New().String())
}

// Txn will create a transaction
func (m *MrT) Txn(fn TxnFn) (err error) {
	var txn Txn
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	txn.writeLine = m.writeLine
	defer txn.clear()

	if err = m.writeLine(TransactionLine, m.newCommitID(), nil); err != nil {
		return
	}

	if err = fn(&txn); err != nil {
		m.rollback()
		return
	}

	return m.flush()
}

// Comment will write a comment line
func (m *MrT) Comment(b []byte) (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	// Write the comment line
	if err = m.writeLine(CommentLine, b, nil); err != nil {
		return
	}

	return m.flush()
}

// ForEach will iterate through all the file lines
func (m *MrT) ForEach(fn ForEachFn) (err error) {
	var key, value []byte
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	return m.s.ReadLines(func(buf *bytes.Buffer) (end bool) {
		var lineType byte
		if lineType, err = buf.ReadByte(); err != nil {
			return true
		}

		switch lineType {
		case TransactionLine, CommentLine:
		case PutLine, DeleteLine:
			var b []byte
			if m.mw != nil {
				var r io.Reader
				if r, err = m.mw.Reader(buf); err != nil {
					return true
				}

				if b, err = ioutil.ReadAll(r); err != nil {
					return true
				}
			} else {
				b = buf.Bytes()
			}

			if m.cor {
				key, value = getKVSafe(b)
			} else {
				key, value = getKV(b)
			}

			return fn(lineType, key, value)

		default:
			err = ErrInvalidLine
			return true
		}

		return
	})
}

// ForEachTxn will iterate through all the file transactions
func (m *MrT) ForEachTxn(fn ForEachTxnFn) (err error) {
	var (
		ti    *TxnInfo
		txnID []byte
		key   []byte
		value []byte
	)

	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	if err = m.s.ReadLines(func(buf *bytes.Buffer) (end bool) {
		var lineType byte
		if lineType, err = buf.ReadByte(); err != nil {
			return true
		}

		// Switch on the first byte (line indicator)
		switch lineType {
		case TransactionLine:
			if ti != nil {
				// A transaction item already exists, let's pass it to the func!
				fn(ti)
			}

			// Extract transaction id from the key
			txnID, _ = getKV(buf.Bytes())

			// Parse uuid from transaction id
			tu, err := uuid.ParseStr(string(txnID))
			if err != nil {
				// Something is definitely wrong here
				// TODO: Handle error logging
				journaler.Error("Error parsing transaction: %v", err)
				return
			}

			ti = &TxnInfo{
				ID: string(txnID),
				TS: tu.Time().Unix(),
			}

		case CommentLine:
		case PutLine, DeleteLine:
			if ti == nil {
				return
			}

			var b []byte
			if m.mw != nil {
				var r io.Reader
				if r, err = m.mw.Reader(buf); err != nil {
					return true
				}

				if b, err = ioutil.ReadAll(r); err != nil {
					return true
				}
			} else {
				b = buf.Bytes()
			}

			key, value = getKV(b)
			ti.Actions = append(ti.Actions, newActionInfo(lineType == PutLine, key, value))

		default:
			err = ErrInvalidLine
			return true
		}

		return
	}); err != nil {
		return
	}

	if ti != nil {
		fn(ti)
	}

	return
}

// Archive will archive the current data
func (m *MrT) Archive(populate TxnFn) (err error) {
	var (
		af  *file.File
		txn Txn
	)

	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	m.buf.Reset()

	txn.writeLine = m.writeLine
	defer txn.clear()

	if err = populate(&txn); err != nil {
		return
	}

	// Open our archive file as an appending file
	if af, err = file.OpenFile(path.Join(m.dir, "archive", m.name+".tdb"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err != nil {
		return
	}
	defer af.Close()

	if err = m.s.SeekToStart(); err != nil {
		return
	}

	// Get the first commit
	if err = m.s.ReadLines(getFirstCommit); err != nil {
		return
	}

	// Move back a line so we can include the first commit
	if err = m.s.PrevLine(); err != nil {
		return
	}

	if _, err = io.Copy(af, m.f); err != nil {
		return
	}

	if err = m.f.Close(); err != nil {
		return
	}

	if m.f, err = file.Create(path.Join(m.dir, m.name+".tdb")); err != nil {
		return
	}

	return m.flush()
}

// Close will close MrT
func (m *MrT) Close() (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	m.closed = true
	m.buf = nil
	m.s = nil
	m.ug = nil
	return m.f.Close()
}

// ForEachFn is used for iterating through entries
type ForEachFn func(lineType byte, key, value []byte) (end bool)

// ForEachTxnFn is used for iterating through transactions
type ForEachTxnFn func(ti *TxnInfo) (end bool)

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

func getFirstCommit(buf *bytes.Buffer) (end bool) {
	return buf.Bytes()[0] == TransactionLine
}

// getKV will extract the key and value from a payload
func getKV(b []byte) (key, value []byte) {
	// Set index at 8 to accommodate 8 bytes for key length
	idx := uint64(8)
	// Get key length
	lv := binary.LittleEndian.Uint64(b[0:idx])
	key = b[idx : lv+idx]

	// Increment index past our key bytes
	idx += lv
	// Get value length
	lv = binary.LittleEndian.Uint64(b[idx : idx+8])
	// Increment our index past the value length
	idx += 8

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
