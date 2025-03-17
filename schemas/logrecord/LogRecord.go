// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package logrecord

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type LogRecord struct {
	_tab flatbuffers.Table
}

func GetRootAsLogRecord(buf []byte, offset flatbuffers.UOffsetT) *LogRecord {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &LogRecord{}
	x.Init(buf, n+offset)
	return x
}

func FinishLogRecordBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsLogRecord(buf []byte, offset flatbuffers.UOffsetT) *LogRecord {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &LogRecord{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedLogRecordBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *LogRecord) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *LogRecord) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *LogRecord) Lsn() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *LogRecord) MutateLsn(n uint64) bool {
	return rcv._tab.MutateUint64Slot(4, n)
}

func (rcv *LogRecord) Hlc() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *LogRecord) MutateHlc(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *LogRecord) Crc32Checksum() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *LogRecord) MutateCrc32Checksum(n uint32) bool {
	return rcv._tab.MutateUint32Slot(8, n)
}

func (rcv *LogRecord) OperationType() LogOperationType {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return LogOperationType(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *LogRecord) MutateOperationType(n LogOperationType) bool {
	return rcv._tab.MutateByteSlot(10, byte(n))
}

func (rcv *LogRecord) TxnState() TransactionState {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return TransactionState(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *LogRecord) MutateTxnState(n TransactionState) bool {
	return rcv._tab.MutateByteSlot(12, byte(n))
}

func (rcv *LogRecord) EntryType() LogEntryType {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return LogEntryType(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *LogRecord) MutateEntryType(n LogEntryType) bool {
	return rcv._tab.MutateByteSlot(14, byte(n))
}

func (rcv *LogRecord) TxnId(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *LogRecord) TxnIdLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *LogRecord) TxnIdBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *LogRecord) MutateTxnId(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *LogRecord) PrevTxnWalIndex(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *LogRecord) PrevTxnWalIndexLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *LogRecord) PrevTxnWalIndexBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *LogRecord) MutatePrevTxnWalIndex(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *LogRecord) PayloadType() LogOperationData {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		return LogOperationData(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *LogRecord) MutatePayloadType(n LogOperationData) bool {
	return rcv._tab.MutateByteSlot(20, byte(n))
}

func (rcv *LogRecord) Payload(obj *flatbuffers.Table) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(22))
	if o != 0 {
		rcv._tab.Union(obj, o)
		return true
	}
	return false
}

func LogRecordStart(builder *flatbuffers.Builder) {
	builder.StartObject(10)
}
func LogRecordAddLsn(builder *flatbuffers.Builder, lsn uint64) {
	builder.PrependUint64Slot(0, lsn, 0)
}
func LogRecordAddHlc(builder *flatbuffers.Builder, hlc uint64) {
	builder.PrependUint64Slot(1, hlc, 0)
}
func LogRecordAddCrc32Checksum(builder *flatbuffers.Builder, crc32Checksum uint32) {
	builder.PrependUint32Slot(2, crc32Checksum, 0)
}
func LogRecordAddOperationType(builder *flatbuffers.Builder, operationType LogOperationType) {
	builder.PrependByteSlot(3, byte(operationType), 0)
}
func LogRecordAddTxnState(builder *flatbuffers.Builder, txnState TransactionState) {
	builder.PrependByteSlot(4, byte(txnState), 0)
}
func LogRecordAddEntryType(builder *flatbuffers.Builder, entryType LogEntryType) {
	builder.PrependByteSlot(5, byte(entryType), 0)
}
func LogRecordAddTxnId(builder *flatbuffers.Builder, txnId flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(6, flatbuffers.UOffsetT(txnId), 0)
}
func LogRecordStartTxnIdVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func LogRecordAddPrevTxnWalIndex(builder *flatbuffers.Builder, prevTxnWalIndex flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(7, flatbuffers.UOffsetT(prevTxnWalIndex), 0)
}
func LogRecordStartPrevTxnWalIndexVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func LogRecordAddPayloadType(builder *flatbuffers.Builder, payloadType LogOperationData) {
	builder.PrependByteSlot(8, byte(payloadType), 0)
}
func LogRecordAddPayload(builder *flatbuffers.Builder, payload flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(9, flatbuffers.UOffsetT(payload), 0)
}
func LogRecordEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
