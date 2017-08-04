package mrT

import (
	"bytes"
	"encoding/binary"
	"github.com/missionMeteora/journaler"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/Path94/fileutils/shasher"
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

// isInCurrent will return whether or not a transaction id is within the current file
func (m *MrT) isInCurrent(txnID string) (ok bool) {
	var err error
	if txnID == "" {
		return
	}

	var ptid string
	if ptid, err = peekFirstTxn(m.s); err != nil {
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

func (m *MrT) readArchiveLines(fn func(*bytes.Buffer) error) (err error) {
	var af *file.File
	if af, err = file.Open(path.Join(m.dir, "archive", m.name+".tdb")); err != nil {
		return
	}
	defer af.Close()

	as := seeker.New(af)
	defer as.SetFile(nil)
	return as.ReadLines(fn)
}

func (m *MrT) getToken() (token []byte) {
	token = []byte(m.name)
	if m.mw != nil {
		token = append(token, strings.Join(m.mw.List(), ",")...)
	}

	return
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
func (m *MrT) ForEach(txnID string, archive bool, fn ForEachFn) (err error) {
	fe := newForEacher(txnID, fn, m.mw, m.cor)
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	journaler.Debug("ForEach: %s %v %v", txnID, archive, m.isInCurrent(txnID))
	if archive && !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(fe.processLine); err != nil && !os.IsNotExist(err) {
			return
		}

		fe.state = stateMatch
		err = nil
	}

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	return m.s.ReadLines(fe.processLine)
}

// ForEachRaw will iterate through all the file lines starting from the provided transaction id
func (m *MrT) ForEachRaw(txnID string, archive bool, fn ForEachRawFn) (err error) {
	fe := newRawForEacher(txnID, fn, m.cor)
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		err = errors.ErrIsClosed
		return
	}

	journaler.Debug("ForEachRaw: %s %v %v", txnID, archive, m.isInCurrent(txnID))
	if archive && !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(fe.processLine); err != nil && !os.IsNotExist(err) {
			return
		}

		fe.state = stateMatch
		err = nil
	}

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	if err = m.s.ReadLines(fe.processLine); err != nil {
		return
	}

	return
}

// ForEachTxn will iterate through all the file transactions starting from the provided transaction id
func (m *MrT) ForEachTxn(txnID string, archive bool, fn ForEachTxnFn) (err error) {
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

	if archive && !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(fe.processLine); err != nil && !os.IsNotExist(err) {
			return
		}

		fe.flush()
		fe.state = stateMatch
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
		af       *file.File
		txn      Txn
		firstTxn string
	)

	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	journaler.Debug("Archiving!")
	if firstTxn, err = peekLastTxn(m.s); err != nil {
		return
	}

	journaler.Debug("First txn: %s", firstTxn)
	m.buf.Reset()

	txn.writeLine = m.writeLine
	defer txn.clear()

	if err = txn.writeLine(TransactionLine, []byte(firstTxn), nil); err != nil {
		return
	}

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

// GetFromRaw will get a key and value line from a raw entry
func (m *MrT) GetFromRaw(raw []byte) (key, value []byte, err error) {
	buf := bytes.NewBuffer(raw)
	return getProcessedKV(buf, m.mw, m.cor)
}

func getTmp() (tmpF *os.File, name string, err error) {
	if tmpF, err = ioutil.TempFile("", "mrT"); err != nil {
		return
	}

	var fi os.FileInfo
	if fi, err = tmpF.Stat(); err != nil {
		return
	}

	name = fi.Name()
	return
}

// Import will import a reader
func (m *MrT) Import(r io.Reader, fn ForEachFn) (lastTxn string, err error) {
	var (
		tmpF *os.File
		tmpN string
	)

	if tmpF, tmpN, err = getTmp(); err != nil {
		return
	}
	defer os.Remove(tmpN)

	// Copy from inbound reader to temporary file
	if _, err = io.Copy(tmpF, r); err != nil {
		journaler.Debug("Oh noes: %v", err)
		return
	}

	if _, err = tmpF.Seek(0, os.SEEK_SET); err != nil {
		journaler.Debug("Oh noes: %v", err)
		return
	}

	m.mux.Lock()
	defer m.mux.Unlock()

	// Seek to end before writing
	if err = m.s.SeekToEnd(); err != nil {
		return
	}

	var n int64
	// Parse payload, check for proper token and signature
	if _, n, err = shasher.ParseWithToken(m.getToken(), tmpF, m.f); err != nil {
		journaler.Error("Oh noes: %v", err)
		return
	}

	// Seek back to before we started writing
	if _, err = m.f.Seek(-n, os.SEEK_CUR); err != nil {
		journaler.Debug("Oh noes: %v", err)
		return
	}

	// ForEach
	fe := newForEacher("", fn, m.mw, m.cor)
	if err = m.s.ReadLines(fe.processLine); err != nil {
		return
	}

	lastTxn = fe.ltid
	return
}

// Export will export from a given transaction id
func (m *MrT) Export(txnID string, w io.Writer) (err error) {
	var hw *shasher.HashWriter
	if hw, err = shasher.NewWithToken(w, m.getToken()); err != nil {
		return
	}

	if err = m.ForEachRaw(txnID, txnID != "", func(line []byte) (err error) {
		if _, err = hw.Write(line); err != nil {
			return
		}

		if _, err = hw.Write([]byte{'\n'}); err != nil {
			return
		}

		return
	}); err != nil {
		return
	}

	_, err = hw.Sign()
	return
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

		// Gets us to the beginning of the current line
		if err = s.PrevLine(); err != nil {
			return
		}
	}
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
