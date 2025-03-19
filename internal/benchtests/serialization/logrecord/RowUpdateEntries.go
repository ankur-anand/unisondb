// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package logrecord

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type RowUpdateEntries struct {
	_tab flatbuffers.Table
}

func GetRootAsRowUpdateEntries(buf []byte, offset flatbuffers.UOffsetT) *RowUpdateEntries {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &RowUpdateEntries{}
	x.Init(buf, n+offset)
	return x
}

func FinishRowUpdateEntriesBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsRowUpdateEntries(buf []byte, offset flatbuffers.UOffsetT) *RowUpdateEntries {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &RowUpdateEntries{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedRowUpdateEntriesBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *RowUpdateEntries) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *RowUpdateEntries) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *RowUpdateEntries) Entries(obj *RowUpdateEncoded, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *RowUpdateEntries) EntriesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func RowUpdateEntriesStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func RowUpdateEntriesAddEntries(builder *flatbuffers.Builder, entries flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(entries), 0)
}
func RowUpdateEntriesStartEntriesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func RowUpdateEntriesEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
