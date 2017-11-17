package mrT

import (
	"io"

	"github.com/PathDNA/fileutils/shasher"
	"github.com/itsmontoya/seeker"
)

func newExporter(m *MrT, w io.Writer, txnID string) (e exporter) {
	e.m = m
	e.w = w
	e.txnID = txnID

	e.mf = NewMatch(txnID)
	return
}

type exporter struct {
	txnID string

	m  *MrT
	w  io.Writer
	hw *shasher.HashWriter
	mf *Match
}

func (e *exporter) exportFrom(rsc ReadSeekCloser) (err error) {
	defer rsc.Close()
	s := seeker.New(rsc)

	var ltid string
	if ltid, err = e.m.LastTxn(); err != nil {
		return
	}

	if ltid == e.txnID {
		return ErrNoTxn
	}

	// Find our target transaction
	if err = e.seekToTransaction(s); err != nil {
		return
	}

	if e.hw == nil {
		// Hash writer hasn't been created yet, initialized hash writer
		if e.hw, err = shasher.NewWithToken(e.w, e.m.getToken()); err != nil {
			return
		}
	}

	if _, err = io.Copy(e.hw, rsc); err != nil {
		return
	}

	return
}

func (e *exporter) seekToTransaction(s *seeker.Seeker) (err error) {
	if e.mf.state != statePreMatch {
		// We already matched our transaction, let's ensure we're pointing at the first transaction
		if _, err = nextTxn(s); err == ErrNoTxn && e.txnID == "" {
			err = nil
		}

		return
	}

	// Read lines, calling filter.processLine on each iteration
	if err = s.ReadLines(e.mf.breakOnMatch); err != nil {
		return
	}

	s.PrevLine()

	if e.mf.state == statePreMatch {
		return ErrNoTxn
	}

	return
}
