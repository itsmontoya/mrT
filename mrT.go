package mrT

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path"
	"strings"

	"github.com/Path94/atoms"
	"github.com/PathDNA/cfile"
	"github.com/PathDNA/fileutils/shasher"

	"github.com/itsmontoya/middleware"
	"github.com/itsmontoya/seeker"
	"github.com/missionMeteora/toolkit/errors"
	"github.com/missionMeteora/uuid"
)

const (
	// NilLine represents a zero-value for line types
	NilLine byte = iota
	// TransactionLine is a line with the transaction tag for the data lines following it
	TransactionLine
	// ReplayLine is a line which signals the replay through a transaction
	ReplayLine
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
	// ErrInvalidTxn is returned when an invalid transaction is provided
	ErrInvalidTxn = errors.Error("transaction does not exist")
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

	if mrT.f, err = cfile.New(path.Join(dir, name+".tdb"), 0644); err != nil {
		return
	}

	mrT.f.SyncAfterWriterClose = true

	if mrT.af, err = cfile.New(path.Join(dir, "archive", name+".tdb"), 0644); err != nil {
		return
	}

	mrT.af.SyncAfterWriterClose = true

	mrT.dir = dir
	mrT.name = name

	// Create new uuid generator
	mrT.ug = uuid.NewGen()
	// Create new write buffer
	mrT.lbuf = newLBuf()

	// Create new seeker
	//	mrT.s = seeker.New(mrT.f)
	// Set Mr.T's middleware
	mrT.setMWs(mws)
	// Set last transaction
	if err = mrT.ForEach("", false, func(lt byte, key, val []byte) (err error) {
		if lt != TransactionLine {
			return
		}

		mrT.ltxn.Store(string(key))
		return
	}); err != nil {
		return
	}

	mp = &mrT
	return
}

// MrT is Mister Transaction, he manages file transactions
// He also pities a fool
type MrT struct {
	dir  string
	name string
	// Copy on read
	cor bool

	// Current file
	f *cfile.File
	// Archive file
	af *cfile.File

	ug *uuid.Gen
	mw *middleware.MWs

	lbuf *lbuf
	nbuf [8]byte
	ltxn atoms.String

	closed atoms.Bool
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

func (m *MrT) writeLine(buf *bytes.Buffer, lineType byte, key, value []byte) (err error) {
	// Write line type
	buf.WriteByte(lineType)

	// If this is not a middleware write, use fast-path
	if !m.isMWWrite(lineType) {
		m.writeRawBytes(buf, key, value)
	} else {
		if err = m.writeMWBytes(buf, key, value); err != nil {
			return
		}
	}

	buf.WriteByte('\n')
	return
}

func (m *MrT) writeRawBytes(buf *bytes.Buffer, key, value []byte) (err error) {
	// We don't check for errors because only middleware can cause errors
	m.writeBytes(buf, key)
	m.writeBytes(buf, value)
	return
}

func (m *MrT) writeMWBytes(buf *bytes.Buffer, key, value []byte) (err error) {
	var w *middleware.Writer
	if w, err = m.mw.Writer(buf); err != nil {
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
		return true
	}

	rdr := m.f.Reader()
	defer rdr.Close()
	s := seeker.New(rdr)

	var rtid string
	rtid, err = replayID(s)
	if err == nil && rtid == txnID {
		return true
	}

	var ptid string
	if ptid, err = peekFirstTxn(s); err != nil {
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

func (m *MrT) readArchiveLines(fn func(*bytes.Buffer) error) (err error) {
	ar := m.af.Reader()
	defer ar.Close()
	as := seeker.New(ar)
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

func (m *MrT) newTxnID() string {
	return m.ug.New().String()
}

// filter will iterate through filtered lines
func (m *MrT) filter(txnID string, archive bool, fn FilterFn, filters []Filter) (err error) {
	f := newFilter(fn, filters)
	curR := m.f.Reader()
	defer curR.Close()
	s := seeker.New(curR)

	if archive && !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(f.processLine); err == nil {
			if _, err = nextTxn(s); err == ErrNoTxn {
				// We do not have any new transactions after our replay id, no need to read from current
				return nil
			} else if err != nil {
				return
			}
		} else if os.IsNotExist(err) {
			// No archive exists, we can still pull from current though
			err = nil
		} else {
			return
		}
	}

	if err = s.ReadLines(f.processLine); err != nil && os.IsNotExist(err) {
		err = nil
	}

	return
}

func (m *MrT) processLine(buf *bytes.Buffer) (lineType byte, key, value []byte, err error) {
	if lineType, err = buf.ReadByte(); err != nil {
		// Error encountered getting line type
		return
	}

	switch lineType {
	case TransactionLine:
		key, value = getKV(buf.Bytes())

	case CommentLine, ReplayLine:
		key, value = getKV(buf.Bytes())

	case PutLine, DeleteLine:
		key, value, err = getProcessedKV(buf, m.mw, m.cor)

	default:
		err = ErrInvalidLine
	}

	return
}

func (m *MrT) parseImportPayload(w *os.File, r io.Reader) (err error) {
	var (
		tmpF *os.File
		tmpN string
	)

	if tmpF, tmpN, err = getTmp(); err != nil {
		return
	}
	defer os.RemoveAll(tmpN)
	defer tmpF.Close()

	if _, err = io.Copy(tmpF, r); err != nil {
		return
	}

	if _, err = tmpF.Seek(0, io.SeekStart); err != nil {
		return
	}

	// Parse payload, check for proper token and signature
	if _, _, err = shasher.ParseWithToken(m.getToken(), tmpF, w); err != nil {
		return
	}

	_, err = w.Seek(0, io.SeekStart)
	return
}

func (m *MrT) appendImportPayload(f *os.File) (err error) {
	// Acquire an appender
	a := m.f.Appender()
	defer a.Close()
	// Copy payload to appender
	if _, err = io.Copy(a, f); err != nil {
		return
	}
	// Reset position before being used again
	_, err = f.Seek(0, io.SeekStart)
	return
}

func (m *MrT) exportArchive(e *exporter) (err error) {
	rdr := m.af.Reader()
	defer rdr.Close()

	err = e.exportFrom(rdr)
	switch {
	case err == ErrNoTxn:
		err = ErrInvalidTxn
	case os.IsNotExist(err):
		err = nil
	}

	return
}

// Txn will create a transaction
func (m *MrT) Txn(fn TxnFn) (err error) {
	// Get a new appender
	a := m.f.Appender()
	// Defer closing the appender
	defer a.Close()
	// Check is MrT is closed
	if m.closed.Get() {
		return errors.ErrIsClosed
	}
	// Assign a new transaction id
	txnID := m.newTxnID()
	// Lock buffer to write to and flush
	if err = m.lbuf.Update(func(buf *bytes.Buffer) (err error) {
		txn := newTxn(buf, m.writeLine)
		defer txn.clear()

		if err = m.writeLine(buf, TransactionLine, []byte(txnID), nil); err != nil {
			return
		}

		if err = fn(&txn); err != nil {
			// We encountered an error while calling func, avoid writing
			return
		}

		_, err = a.Write(buf.Bytes())
		return
	}); err != nil {
		return
	}

	if err = a.Sync(); err != nil {
		return
	}

	m.ltxn.Store(txnID)
	return
}

// Comment will write a comment line
func (m *MrT) Comment(b []byte) (err error) {
	a := m.f.Appender()
	defer a.Close()
	if m.closed.Get() {
		return errors.ErrIsClosed
	}

	return m.lbuf.Update(func(buf *bytes.Buffer) (err error) {
		// Write the comment line
		if err = m.writeLine(buf, CommentLine, b, nil); err != nil {
			return
		}

		_, err = a.Write(buf.Bytes())
		return
	})
}

// Filter will iterate through filtered lines
func (m *MrT) Filter(txnID string, archive bool, fn FilterFn, filters ...Filter) (err error) {
	if m.closed.Get() {
		return errors.ErrIsClosed
	}

	if txnID != "" && txnID == m.ltxn.Load() {
		return
	}

	return m.filter(txnID, archive, fn, filters)
}

// ForEach will iterate through all the file lines starting from the provided transaction id
func (m *MrT) ForEach(txnID string, archive bool, fn ForEachFn) (err error) {
	match := NewMatch(txnID)
	return m.Filter(txnID, archive, func(buf *bytes.Buffer) (err error) {
		var (
			lineType   byte
			key, value []byte
		)

		if lineType, key, value, err = m.processLine(buf); err != nil {
			return
		}

		return fn(lineType, key, value)
	}, match)
}

// ForEachRaw will iterate through all the raw file lines starting from the provided transaction id
func (m *MrT) ForEachRaw(txnID string, archive bool, fn ForEachRawFn) (err error) {
	match := NewMatch(txnID)
	return m.Filter(txnID, archive, func(buf *bytes.Buffer) (err error) {
		return fn(buf.Bytes())
	}, match)
}

// ForEachTxn will iterate through all the file transactions starting from the provided transaction id
func (m *MrT) ForEachTxn(txnID string, archive bool, fn ForEachTxnFn) (err error) {
	fe := newTxnForEacher(txnID, fn, m.mw)
	if m.closed.Get() {
		return errors.ErrIsClosed
	}

	rdr := m.f.Reader()
	defer rdr.Close()
	s := seeker.New(rdr)

	if archive && !m.isInCurrent(txnID) {
		if err = m.readArchiveLines(fe.processLine); err != nil && !os.IsNotExist(err) {
			return
		}

		fe.flush()
		fe.state = stateMatch
		err = nil
	}

	if err = s.ReadLines(fe.processLine); err != nil {
		return
	}

	fe.flush()
	return
}

// LastTxn will get the last transaction id
func (m *MrT) LastTxn() (txnID string, err error) {
	if m.closed.Get() {
		err = errors.ErrIsClosed
		return
	}

	txnID = m.ltxn.Load()
	return
}

// Archive will archive the current data
func (m *MrT) Archive(populate TxnFn) (err error) {
	if err = m.f.With(func(f *os.File) (err error) {
		if m.closed.Get() {
			return errors.ErrIsClosed
		}

		aw := m.af.Writer()
		defer aw.Close()

		// Ensure archive file is at the end
		if _, err = aw.Seek(0, io.SeekEnd); err != nil {
			return
		}

		lastTxn := m.ltxn.Load()

		f.Seek(0, io.SeekStart)
		s := seeker.New(f)

		// Get the first commit
		if err = s.ReadLines(getFirstCommit); err != nil {
			return
		}

		// Move back a line so we can include the first commit
		if err = s.PrevLine(); err != nil {
			return
		}

		if _, err = io.Copy(aw, f); err != nil {
			return
		}

		// AT END
		if err = f.Truncate(0); err != nil {
			return
		}

		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return
		}

		// TODO: CLEAN UP THIS LOCKCEPTION
		if err = m.lbuf.Update(func(buf *bytes.Buffer) (err error) {
			txn := newTxn(buf, m.writeLine)
			defer txn.clear()

			if err = txn.writeLine(buf, ReplayLine, []byte(lastTxn), nil); err != nil {
				return
			}

			if err = populate(&txn); err != nil {
				return
			}

			if _, err = f.Write(buf.Bytes()); err != nil {
				return
			}

			return f.Sync()
		}); err != nil {
			return
		}

		return
	}); err != nil {
		return
	}

	return
}

// GetFromRaw will get a key and value line from a raw entry
func (m *MrT) GetFromRaw(raw []byte) (key, value []byte, err error) {
	buf := bytes.NewBuffer(raw)
	return getProcessedKV(buf, m.mw, m.cor)
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

	if err = m.parseImportPayload(tmpF, r); err != nil {
		return
	}

	if err = m.appendImportPayload(tmpF); err != nil {
		return
	}

	s := seeker.New(tmpF)
	err = s.ReadLines(func(buf *bytes.Buffer) (err error) {
		var (
			lineType byte
			key, val []byte
		)

		if lineType, key, val, err = m.processLine(buf); err != nil {
			return
		}

		if lineType == TransactionLine || lineType == ReplayLine {
			lastTxn = string(key)
		}

		return fn(lineType, key, val)
	})

	return
}

// Export will export from a given transaction id
func (m *MrT) Export(txnID string, w io.Writer) (err error) {
	if txnID != "" && txnID == m.ltxn.Load() {
		return ErrNoTxn
	}

	e := newExporter(m, w, txnID)
	// Assign current reader to aquire read-lock for file
	cr := m.f.Reader()
	defer cr.Close()

	if txnID == "" || !m.isInCurrent(txnID) {
		if err = m.exportArchive(&e); err != nil {
			return
		}
	}

	if err = e.exportFrom(cr); err != nil {
		return
	}

	if e.hw != nil {
		_, err = e.hw.Sign()
	}

	return
}

// Close will close MrT
func (m *MrT) Close() (err error) {
	if !m.closed.Set(true) {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(m.f.Close())
	errs.Push(m.af.Close())

	m.lbuf = nil
	m.ug = nil
	return errs.Err()
}
