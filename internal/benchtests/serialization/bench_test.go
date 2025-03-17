package serialization

import (
	"encoding/json"
	"testing"

	"github.com/ankur-anand/kvalchemy/dbkernel/wal/walrecord"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

func BenchmarkFlatBuffersEncodeDecode(b *testing.B) {
	wr := &walrecord.Record{
		Index:         100,
		Hlc:           200,
		LogOperation:  walrecord.LogOperationInsert,
		TxnStatus:     walrecord.TxnStatusCommit,
		EntryType:     walrecord.ValueTypeColumn,
		Key:           []byte("example-key"),
		Value:         []byte("example-value"),
		TxnID:         []byte("txn-123"),
		PrevTxnOffset: nil,
		ColumnEntries: map[string][]byte{
			"column1":  []byte("value"),
			"column2":  []byte("value"),
			"column3":  []byte("value"),
			"column4":  []byte("value"),
			"column5":  []byte("value"),
			"column6":  []byte("value"),
			"column7":  []byte("value"),
			"column8":  []byte("value"),
			"column9":  []byte("value"),
			"column10": []byte("value"),
		},
	}

	b.Run("FlatBuffers Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoded, _ := wr.FBEncode()
			size := len(encoded)
			b.ReportMetric(float64(size), "bytes/op")
		}
	})

	b.Run("FlatBuffers Decode", func(b *testing.B) {
		encoded, _ := wr.FBEncode()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			fb := walrecord.GetRootAsWalRecord(encoded, 0)
			_ = fb
			b.ReportMetric(float64(len(encoded)), "bytes/op")
		}
	})
}

func BenchmarkProtoBufEncodeDecode(b *testing.B) {
	wr := &WalRecord{
		Index:           100,
		Hlc:             200,
		Operation:       LogOperation_INSERT,
		TxnStatus:       TxnStatus_COMMIT,
		ValueType:       ValueType_FULL,
		Key:             []byte("example-key"),
		Value:           []byte("example-value"),
		TxnId:           []byte("txn-123"),
		PrevTxnWalIndex: nil,
		Columns: []*ColumnEntry{
			{
				ColumnName:  "column1",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column2",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column3",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column4",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column5",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column6",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column7",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column8",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column9",
				ColumnValue: []byte("value"),
			},
			{
				ColumnName:  "column10",
				ColumnValue: []byte("value"),
			},
		},
	}

	b.Run("ProtoBuf Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoded, _ := proto.Marshal(wr)
			size := len(encoded)
			b.ReportMetric(float64(size), "bytes/op")
		}
	})

	b.Run("ProtoBuf Decode", func(b *testing.B) {
		encoded, _ := proto.Marshal(wr)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = proto.Unmarshal(encoded, &WalRecord{})
			b.ReportMetric(float64(len(encoded)), "bytes/op")
		}
	})
}

func BenchmarkJSONEncodeDecode(b *testing.B) {
	wr := &walrecord.Record{
		Index:         100,
		Hlc:           200,
		LogOperation:  walrecord.LogOperationInsert,
		TxnStatus:     walrecord.TxnStatusCommit,
		EntryType:     walrecord.ValueTypeFull,
		Key:           []byte("example-key"),
		Value:         []byte("example-value"),
		TxnID:         []byte("txn-123"),
		PrevTxnOffset: nil,
		ColumnEntries: map[string][]byte{
			"column1":  []byte("value"),
			"column2":  []byte("value"),
			"column3":  []byte("value"),
			"column4":  []byte("value"),
			"column5":  []byte("value"),
			"column6":  []byte("value"),
			"column7":  []byte("value"),
			"column8":  []byte("value"),
			"column9":  []byte("value"),
			"column10": []byte("value"),
		},
	}

	b.Run("JSON Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoded, _ := json.Marshal(wr)
			size := len(encoded)
			b.ReportMetric(float64(size), "bytes/op")
		}
	})

	b.Run("JSON Decode", func(b *testing.B) {
		encoded, _ := json.Marshal(wr)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = json.Unmarshal(encoded, &walrecord.Record{})
			b.ReportMetric(float64(len(encoded)), "bytes/op")
		}
	})
}

func BenchmarkMsgPackEncodeDecode(b *testing.B) {
	wr := &walrecord.Record{
		Index:         100,
		Hlc:           200,
		LogOperation:  walrecord.LogOperationInsert,
		TxnStatus:     walrecord.TxnStatusCommit,
		EntryType:     walrecord.ValueTypeFull,
		Key:           []byte("example-key"),
		Value:         []byte("example-value"),
		TxnID:         []byte("txn-123"),
		PrevTxnOffset: nil,
		ColumnEntries: map[string][]byte{
			"column1":  []byte("value"),
			"column2":  []byte("value"),
			"column3":  []byte("value"),
			"column4":  []byte("value"),
			"column5":  []byte("value"),
			"column6":  []byte("value"),
			"column7":  []byte("value"),
			"column8":  []byte("value"),
			"column9":  []byte("value"),
			"column10": []byte("value"),
		},
	}

	b.Run("MsgPack Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoded, _ := msgpack.Marshal(wr)
			size := len(encoded)
			b.ReportMetric(float64(size), "bytes/op")
		}
	})

	b.Run("MsgPack Decode", func(b *testing.B) {
		encoded, _ := msgpack.Marshal(wr)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = msgpack.Unmarshal(encoded, &walrecord.Record{})
			b.ReportMetric(float64(len(encoded)), "bytes/op")
		}
	})
}
