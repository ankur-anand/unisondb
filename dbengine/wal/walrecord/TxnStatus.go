// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package walrecord

import "strconv"

type TxnStatus byte

const (
	TxnStatusTxnNone TxnStatus = 0
	TxnStatusBegin   TxnStatus = 1
	TxnStatusPrepare TxnStatus = 2
	TxnStatusCommit  TxnStatus = 3
)

var EnumNamesTxnStatus = map[TxnStatus]string{
	TxnStatusTxnNone: "TxnNone",
	TxnStatusBegin:   "Begin",
	TxnStatusPrepare: "Prepare",
	TxnStatusCommit:  "Commit",
}

var EnumValuesTxnStatus = map[string]TxnStatus{
	"TxnNone": TxnStatusTxnNone,
	"Begin":   TxnStatusBegin,
	"Prepare": TxnStatusPrepare,
	"Commit":  TxnStatusCommit,
}

func (v TxnStatus) String() string {
	if s, ok := EnumNamesTxnStatus[v]; ok {
		return s
	}
	return "TxnStatus(" + strconv.FormatInt(int64(v), 10) + ")"
}
