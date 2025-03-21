package memtable

import (
	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/dgraph-io/badger/v4/y"
)

// BuildColumnMap builds the columns from the provided mem-table entries.
// it modifies the provided columnEntries with the entries fetched from mem table.
func BuildColumnMap(columnEntries map[string][]byte, vs []y.ValueStruct) {
	for _, v := range vs {
		re := extractColumns(v.Value)
		switch v.Meta {
		case internal.LogOperationInsert:
			for cn, cv := range re.Columns {
				columnEntries[cn] = cv
			}
		case internal.LogOperationDelete:
			for cn := range re.Columns {
				delete(columnEntries, cn)
			}
		}
	}
}

// extractColumns gets all the column entries from the wal log record.
func extractColumns(data []byte) logcodec.RowEntry {
	return logcodec.DeserializeRowUpdateEntry(data)
}
