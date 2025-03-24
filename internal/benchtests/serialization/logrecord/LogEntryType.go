// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package logrecord

import "strconv"

type LogEntryType byte

const (
	LogEntryTypeKV      LogEntryType = 0
	LogEntryTypeChunked LogEntryType = 1
	LogEntryTypeRow     LogEntryType = 2
)

var EnumNamesLogEntryType = map[LogEntryType]string{
	LogEntryTypeKV:      "KV",
	LogEntryTypeChunked: "Chunked",
	LogEntryTypeRow:     "Row",
}

var EnumValuesLogEntryType = map[string]LogEntryType{
	"KV":      LogEntryTypeKV,
	"Chunked": LogEntryTypeChunked,
	"Row":     LogEntryTypeRow,
}

func (v LogEntryType) String() string {
	if s, ok := EnumNamesLogEntryType[v]; ok {
		return s
	}
	return "LogEntryType(" + strconv.FormatInt(int64(v), 10) + ")"
}
