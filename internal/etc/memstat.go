// Copyright (c) HashiCorp, Inc and kvalchemy authors.
// SPDX-License-Identifier: MIT

// nolint
package etc

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/hashicorp/go-metrics"
)

// Wanted to dump metrics for testing purpose, not on Signal, as Provided by Hashiscorp.

// DumpStats is used to dump the data to output writer.
func DumpStats(inm *metrics.InmemSink, w io.Writer) error {
	buf := bytes.NewBuffer(nil)

	data := inm.Data()
	// Skip the last period which is still being aggregated
	for j := 0; j < len(data)-1; j++ {
		intv := data[j]
		intv.RLock()
		for _, val := range intv.Gauges {
			name := flattenLabels(val.Name, val.Labels)
			fmt.Fprintf(buf, "[%v][G] '%s': %0.3f\n", intv.Interval, name, val.Value)
		}
		for _, val := range intv.PrecisionGauges {
			name := flattenLabels(val.Name, val.Labels)
			fmt.Fprintf(buf, "[%v][G] '%s': %0.3f\n", intv.Interval, name, val.Value)
		}
		for name, vals := range intv.Points {
			for _, val := range vals {
				fmt.Fprintf(buf, "[%v][P] '%s': %0.3f\n", intv.Interval, name, val)
			}
		}
		for _, agg := range intv.Counters {
			name := flattenLabels(agg.Name, agg.Labels)
			fmt.Fprintf(buf, "[%v][C] '%s': %s\n", intv.Interval, name, agg.AggregateSample)
		}
		for _, agg := range intv.Samples {
			name := flattenLabels(agg.Name, agg.Labels)
			fmt.Fprintf(buf, "[%v][S] '%s': %s\n", intv.Interval, name, agg.AggregateSample)
		}
		intv.RUnlock()
	}

	// Write out the bytes
	w.Write(buf.Bytes())
	return nil
}

// Flattens the key for formatting along with its labels, removes spaces.
func flattenLabels(name string, labels []metrics.Label) string {
	buf := bytes.NewBufferString(name)
	replacer := strings.NewReplacer(" ", "_", ":", "_")

	for _, label := range labels {
		replacer.WriteString(buf, ".")
		replacer.WriteString(buf, label.Value)
	}

	return buf.String()
}
