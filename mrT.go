package mrT

import (
	"bytes"
	"io"
	"os"
	"path"
	"sync"

	"github.com/itsmontoya/async/file"
	"github.com/itsmontoya/seeker"
	"github.com/missionMeteora/toolkit/errors"
	"github.com/missionMeteora/uuid"
)

const (
	// NilLine represents a zero-value for line types
	NilLine byte = iota
	// CommitLine is the line with the commit tag for the data line following it
	CommitLine
	// CommentLine is a comment line, will be ignored when parsing data
	// Note: This line type will always ignore middleware
	CommentLine
	// DataLine is a line containing data
	DataLine
)

const (
	// ErrInvalidLine is returned when an invalid line is encountered while parsing
	ErrInvalidLine = errors.Error("invalid line")
)

// New will return a new instance of MrT
func New(dir, name string) (mp *MrT, err error) {
	var mrT MrT
	if mrT.f, err = file.Create(path.Join(dir, name+".tdb")); err != nil {
		return
	}

	mrT.dir = dir
	mrT.name = name

	mrT.ug = uuid.NewGen()
	mrT.buf = bytes.NewBuffer(nil)
	mrT.s = seeker.New(mrT.f)
	mp = &mrT
	return
}

// MrT is Mister Transaction, he manages file transactions
// He also pities a fool
type MrT struct {
	mux sync.RWMutex

	dir  string
	name string

	f   *file.File
	s   *seeker.Seeker
	buf *bytes.Buffer

	ug *uuid.Gen
}

func (m *MrT) writeData(data []byte) {
	m.write(DataLine, data)
}

func (m *MrT) write(lineType byte, data []byte) {
	m.buf.WriteByte(lineType)
	m.buf.Write(data)
	m.buf.WriteByte('\n')
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

// Commit will create a commit
func (m *MrT) Commit(fn CommitFn) (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	cid := m.newCommitID()
	m.write(CommitLine, cid)
	if err = fn(m.writeData); err != nil {
		m.rollback()
		return
	}

	return m.flush()
}

// Comment will write a comment line
func (m *MrT) Comment(b []byte) (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	// Write the comment line
	m.write(CommentLine, b)

	return m.flush()
}

// ForEach will iterate through all the file lines
func (m *MrT) ForEach(fn ForEachFn) (err error) {
	var (
		cid  string
		data []byte
	)

	m.mux.Lock()
	defer m.mux.Unlock()

	if err = m.s.SeekToStart(); err != nil {
		return
	}
	defer m.s.SeekToEnd()

	return m.s.ReadLines(func(buf *bytes.Buffer) (end bool) {
		b := buf.Bytes()
		switch b[0] {
		case CommitLine:
			cid = string(b[1:])
			return

		case DataLine:
			data = make([]byte, len(b)-1)
			copy(data, b[1:])
			return fn(cid, data)

		default:
			err = ErrInvalidLine
			return true
		}
	})
}

// Archive will archive the current data
func (m *MrT) Archive(populate CommitFn) (err error) {
	var (
		af *file.File
	)

	m.mux.Lock()
	defer m.mux.Unlock()
	defer m.buf.Reset()

	if err = populate(m.writeData); err != nil {
		return
	}

	// Open our archive file as an appending file
	if af, err = file.OpenFile(path.Join(m.dir, m.name+".archive.tdb"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err != nil {
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

// ForEachFn is used for for each'ing through commits
type ForEachFn func(commitID string, data []byte) (end bool)

// WriteFn is used for writing lines
type WriteFn func([]byte)

// CommitFn is used for commiting
type CommitFn func(write WriteFn) error

func getFirstCommit(buf *bytes.Buffer) (end bool) {
	return buf.Bytes()[0] == CommitLine
}
