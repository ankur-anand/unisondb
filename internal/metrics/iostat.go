package metrics

import (
	"log/slog"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
)

const (
	namespace = "unisondb"
	subsystem = "process"
)

// IOStatsCollector collects I/O statistics.
type IOStatsCollector struct {
	proc *process.Process
	mu   sync.Mutex

	writeBytesDesc *prometheus.Desc

	cpuIowaitDesc *prometheus.Desc
}

// NewIOStatsCollector creates a new I/O stats collector
func NewIOStatsCollector() (*IOStatsCollector, error) {
	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}

	return &IOStatsCollector{
		proc: proc,
		writeBytesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "io_write_bytes_total"),
			"Total number of bytes written by the process",
			nil, nil,
		),
		cpuIowaitDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "system", "cpu_iowait_percent"),
			"Percentage of CPU time spent waiting for I/O",
			nil, nil,
		),
	}, nil
}

// Describe implements prometheus.Collector
func (c *IOStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.writeBytesDesc
	ch <- c.cpuIowaitDesc
}

// Collect implements prometheus.Collector
func (c *IOStatsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ioCounters, err := c.proc.IOCounters()
	if err != nil {
		slog.Debug("Failed to get process I/O counters", "error", err)
	} else {
		ch <- prometheus.MustNewConstMetric(
			c.writeBytesDesc,
			prometheus.CounterValue,
			float64(ioCounters.WriteBytes),
		)
	}

	cpuPercent, err := cpu.Times(false)
	if err != nil {
		slog.Debug("Failed to get CPU times", "error", err)
		return
	}

	if len(cpuPercent) == 0 {
		return
	}

	times := cpuPercent[0]
	total := times.User + times.System + times.Idle + times.Nice +
		times.Iowait + times.Irq + times.Softirq + times.Steal

	var iowaitPercent float64
	if total > 0 {
		iowaitPercent = (times.Iowait / total) * 100.0
	}

	ch <- prometheus.MustNewConstMetric(
		c.cpuIowaitDesc,
		prometheus.GaugeValue,
		iowaitPercent,
	)
}
