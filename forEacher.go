package mrT

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/itsmontoya/middleware"
	"github.com/missionMeteora/journaler"
	"github.com/missionMeteora/uuid"
)

// forEachState represents the match state of a ForEach/ForEachTx request
type forEachState uint8

const (
	// statePreMatch represents an iteration which has not yet matched the target transaction
	statePreMatch forEachState = iota
	// stateMatch represents an iteration which has matched the target transaction
	stateMatch
	// statePostMatch represents an iteration which has matched and moved past the target transaction (ready to append)
	statePostMatch
)

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

func (fe *txnForEacher) processLine(buf *bytes.Buffer) (err error) {
	var (
		lineType   byte
		key, value []byte
		// Transaction id
		tid []byte
	)

	if lineType, err = buf.ReadByte(); err != nil {
		return
	}

	// Switch on the first byte (line indicator)
	switch lineType {
	case TransactionLine, ReplayLine:
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
		var tu uuid.UUID
		if tu, err = uuid.ParseStr(string(tid)); err != nil {
			// Something is definitely wrong here (almost enough to panic)
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

		if key, value, err = getProcessedKV(buf, fe.mw, true); err != nil {
			return
		}

		fe.ti.Actions = append(fe.ti.Actions, newActionInfo(lineType == PutLine, key, value))

	default:
		err = ErrInvalidLine
		return
	}

	return
}

type forEachKVFn func(buf *bytes.Buffer, mw *middleware.MWs, cor bool) (key, val []byte, err error)

func getProcessedKV(buf *bytes.Buffer, mw *middleware.MWs, cor bool) (key, val []byte, err error) {
	var b []byte
	if mw != nil {
		var r io.Reader
		if r, err = mw.Reader(buf); err != nil {
			return
		}

		if b, err = ioutil.ReadAll(r); err != nil {
			return
		}
	} else {
		b = buf.Bytes()
	}

	if !cor {
		key, val = getKV(b)
	} else {
		key, val = getKVSafe(b)
	}

	return
}
