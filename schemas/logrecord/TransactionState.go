// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package logrecord

import "strconv"

type TransactionState byte

const (
	TransactionStateNone    TransactionState = 0
	TransactionStateBegin   TransactionState = 1
	TransactionStatePrepare TransactionState = 2
	TransactionStateCommit  TransactionState = 3
)

var EnumNamesTransactionState = map[TransactionState]string{
	TransactionStateNone:    "None",
	TransactionStateBegin:   "Begin",
	TransactionStatePrepare: "Prepare",
	TransactionStateCommit:  "Commit",
}

var EnumValuesTransactionState = map[string]TransactionState{
	"None":    TransactionStateNone,
	"Begin":   TransactionStateBegin,
	"Prepare": TransactionStatePrepare,
	"Commit":  TransactionStateCommit,
}

func (v TransactionState) String() string {
	if s, ok := EnumNamesTransactionState[v]; ok {
		return s
	}
	return "TransactionState(" + strconv.FormatInt(int64(v), 10) + ")"
}
