package mrT

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/itsmontoya/middleware"
	"github.com/missionMeteora/journaler"
	"github.com/missionMeteora/uuid"
)

type forEachState uint8

const (
	statePreMatch forEachState = iota
	stateMatch
	statePostMatch
)

func newForEacher(tid string, fn ForEachFn, mw *middleware.MWs, cor bool) *forEacher {
	var fe forEacher
	fe.tid = tid
	fe.fn = fn
	fe.mw = mw
	fe.cor = cor

	if tid == "" {
		fe.state = statePostMatch
	} else {
		fe.state = statePreMatch
	}

	return &fe
}

type forEacher struct {
	tid string
	fn  ForEachFn
	mw  *middleware.MWs
	cor bool
	// Match state
	state forEachState
}

func (fe *forEacher) processLine(buf *bytes.Buffer) (end bool) {
	var (
		lineType byte
		key      []byte
		value    []byte
		err      error
	)

	if lineType, err = buf.ReadByte(); err != nil {
		return true
	}

	switch lineType {
	case TransactionLine, CommentLine:
		if fe.state == statePreMatch {
			// Extract transaction id from the key
			ctid, _ := getKV(buf.Bytes())
			if string(ctid) == fe.tid {
				fe.state = stateMatch
			}
		} else if fe.state == stateMatch {
			fe.state = statePostMatch
		}

	case PutLine, DeleteLine:
		if fe.state != statePostMatch {
			return
		}

		var b []byte
		if fe.mw != nil {
			var r io.Reader
			if r, err = fe.mw.Reader(buf); err != nil {
				return true
			}

			if b, err = ioutil.ReadAll(r); err != nil {
				return true
			}
		} else {
			b = buf.Bytes()
		}

		if fe.cor {
			key, value = getKVSafe(b)
		} else {
			key, value = getKV(b)
		}

		return fe.fn(lineType, key, value)

	default:
		err = ErrInvalidLine
		return true
	}

	return
}

func newTxnForEacher(tid string, fn ForEachTxnFn, mw *middleware.MWs) *txnForEacher {
	var fe txnForEacher
	fe.tid = tid
	fe.fn = fn
	fe.mw = mw

	if tid == "" {
		fe.state = statePostMatch
	} else {
		fe.state = statePreMatch
	}

	return &fe
}

type txnForEacher struct {
	tid string
	fn  ForEachTxnFn
	ti  *TxnInfo
	mw  *middleware.MWs
	// Match state
	state forEachState
}

func (fe *txnForEacher) flush() {
	if fe.ti == nil {
		return
	}

	// A transaction item already exists, let's pass it to the func!
	// Note: We will be replacing this with the other new function at the end of this case
	fe.fn(fe.ti)
	fe.ti = nil
}

func (fe *txnForEacher) processLine(buf *bytes.Buffer) (end bool) {
	var (
		lineType byte

		tid   []byte
		key   []byte
		value []byte
		err   error
	)

	if lineType, err = buf.ReadByte(); err != nil {
		return true
	}

	// Switch on the first byte (line indicator)
	switch lineType {
	case TransactionLine:
		fe.flush()
		// Extract transaction id from the key
		tid, _ = getKV(buf.Bytes())
		if fe.state == statePreMatch {
			if fe.tid == string(tid) {
				fe.state = stateMatch
			}

			return
		} else if fe.state == stateMatch {
			fe.state = statePostMatch
		}

		// Parse uuid from transaction id
		tu, err := uuid.ParseStr(string(tid))
		if err != nil {
			// Something is definitely wrong here
			journaler.Error("Error parsing transaction: %v", err)
			return
		}

		fe.ti = &TxnInfo{
			ID: string(tid),
			TS: tu.Time().Unix(),
		}

	case CommentLine:
	case PutLine, DeleteLine:
		if fe.ti == nil {
			return
		}

		if fe.state != statePostMatch {
			return
		}

		var b []byte
		if fe.mw != nil {
			var r io.Reader
			if r, err = fe.mw.Reader(buf); err != nil {
				return true
			}

			if b, err = ioutil.ReadAll(r); err != nil {
				return true
			}
		} else {
			b = buf.Bytes()
		}

		key, value = getKV(b)
		fe.ti.Actions = append(fe.ti.Actions, newActionInfo(lineType == PutLine, key, value))

	default:
		err = ErrInvalidLine
		return true
	}

	return
}
