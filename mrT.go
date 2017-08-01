package mrT

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path"
	"sync"

	"github.com/itsmontoya/async/file"
	"github.com/itsmontoya/middleware"
	"github.com/itsmontoya/seeker"
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
	// ErrNoTxn is returned when no transactions are available
	ErrNoTxn = errors.Error("no transactions available")
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

// peekFirstTxn will return the first transaction id within the current file
func (m *MrT) peekFirstTxn() (txnID string, err error) {
	if err = m.s.SeekToStart(); err != nil {
		return
	}

	rerr := m.s.ReadLines(func(buf *bytes.Buffer) (end bool) {
		var lineType byte
		if lineType, err = buf.ReadByte(); err != nil {
			return true
		}

		if lineType != TransactionLine {
			return
		}

		tidb, _ := getKV(buf.Bytes())
		txnID = string(tidb)
		return true
	})

	if err == nil && rerr != nil {
		err = rerr
		return
	}

	if txnID == "" {
		err = ErrNoTxn
		return
	}

	return
}

// isInCurrent will return whether or not a transaction id is within the current file
func (m *MrT) isInCurrent(txnID string) (ok bool) {
	var err error
	if txnID == "" {
		return
	}

	var ptid string
	if ptid, err = m.peekFirstTxn(); err != nil {
		return
	}

	var ru, pu uuid.UUID
	if ru, err = uuid.ParseStr(txnID); err != nil {
		return
	}

	if pu, err = uuid.ParseStr(ptid); err != nil {
		return
	}

	return ru.Time().UnixNano() >= pu.Time().UnixNano()
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

func (m *MrT) readArchiveLines(fn func(*bytes.Buffer) bool) (err error) {
	var af *file.File
	if af, err = file.Open(path.Join(m.dir, "archive", m.name+".tdb")); err != nil {
		return
	}
	defer af.Close()

	as := seeker.New(af)
	defer as.SetFile(nil)
	return as.ReadLines(fn)
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

// ForEach will iterate through all the file lines starting from the provided transaction id
func (m *MrT) ForEach(txnID string, fn ForEachFn) (err error) {
	fe := newForEacher(txnID, fn, m.mw, m.cor)
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	if !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(fe.processLine); err != nil && !os.IsNotExist(err) {
			return
		}

		fe.state = 0
		err = nil
	}

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	return m.s.ReadLines(fe.processLine)
}

// ForEachTxn will iterate through all the file transactions starting from the provided transaction id
func (m *MrT) ForEachTxn(txnID string, fn ForEachTxnFn) (err error) {
	fe := newTxnForEacher(txnID, fn, m.mw)
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	if !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(fe.processLine); err != nil && !os.IsNotExist(err) {
			return
		}

		fe.flush()
		fe.state = 0
		err = nil
	}

	if err = m.s.ReadLines(fe.processLine); err != nil {
		return
	}

	fe.flush()
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
	m.s.SetFile(m.f)

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
