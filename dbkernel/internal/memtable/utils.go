package memtable

import (
	"fmt"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/dgraph-io/badger/v4/y"
)

type tsGenerator struct {
	lastTS uint64
}

func (g *tsGenerator) Next() uint64 {
	now := uint64(time.Now().UnixNano())
	// UnixNano() may not return a strictly increasing value in fast loops,
	if now <= g.lastTS {
		// ensure strictly increasing
		g.lastTS++
	} else {
		g.lastTS = now
	}
	return g.lastTS
}

// BuildColumnMap builds the columns from the provided mem-table entries.
// it modifies the provided columnEntries with the entries fetched from mem table.
func BuildColumnMap(columnEntries map[string][]byte, vs []y.ValueStruct) {
	fmt.Println("build column map called", len(vs))
	for _, v := range vs {
		if v.Meta == internal.LogOperationDeleteRowByKey {
			// reset all key
			for k := range columnEntries {
				delete(columnEntries, k)
			}
			// inbetween ops
			continue
		}
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
