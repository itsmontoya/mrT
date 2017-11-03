package mrT

import (
	"bytes"

	"github.com/itsmontoya/seeker"
)

func newFilter(fn FilterFn, fs []Filter) *filter {
	var f filter
	f.fn = fn
	f.fs = fs
	return &f
}

type filter struct {
	fn FilterFn
	fs []Filter
}

func (f *filter) processLine(buf *bytes.Buffer) (err error) {
	var ok bool
	for _, ff := range f.fs {
		if ok, err = ff.Filter(buf); !ok {
			return
		}
	}

	return f.fn(buf)
}

// Filter is a basic filtering interface
type Filter interface {
	Filter(buf *bytes.Buffer) (ok bool, err error)
}

// FilterFn  is a basic filter fn
type FilterFn func(*bytes.Buffer) error

// NewMatch will return a new match filter
func NewMatch(txnID string) *Match {
	var m Match
	m.tid = txnID

	if txnID == "" {
		m.state = statePostMatch
	}

	return &m
}

// Match is a transaction matching filter
type Match struct {
	// Target transaction id
	tid string
	// Match state
	state forEachState
}

// Filter interface fulfillment
func (m *Match) Filter(buf *bytes.Buffer) (ok bool, err error) {
	// Fast track for post-match state
	if m.state == statePostMatch {
		return true, nil
	}

	var lineType byte
	if lineType, err = getLineType(buf); err != nil {
		return
	}

	switch lineType {
	case TransactionLine, ReplayLine:
		switch m.state {
		case statePreMatch:
			buf.ReadByte()
			kk := getKey(buf.Bytes())
			buf.UnreadByte()

			if m.tid == kk {
				m.state = stateMatch
			}
		case stateMatch:
			m.state = statePostMatch
		}

	case PutLine:
	case DeleteLine:
	case CommentLine:

	default:
		err = ErrInvalidLine
	}

	ok = m.state == statePostMatch
	return
}

func (m *Match) breakOnMatch(buf *bytes.Buffer) (err error) {
	var ok bool
	if ok, err = m.Filter(buf); err != nil {
		return
	}

	if ok {
		return seeker.ErrEndEarly
	}

	return
}
