package dbkernel

import (
	"time"

	"github.com/ankur-anand/unisondb/dbkernel/internal/memtable"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/prometheus/common/helpers/templates"
)

func getValueStruct(ops byte, entryType byte, value []byte) y.ValueStruct {
	return y.ValueStruct{
		Meta:     ops,
		UserMeta: entryType,
		Value:    value,
	}
}

// buildColumnMap builds the columns from the provided mem-table entries.
// it modifies the provided columnEntries with the entries fetched from mem table.
func buildColumnMap(columnEntries map[string][]byte, vs []y.ValueStruct) {
	memtable.BuildColumnMap(columnEntries, vs)
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.String()
	}
	return s
}
