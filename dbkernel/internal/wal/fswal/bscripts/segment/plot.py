import sys
import re
import matplotlib.pyplot as plt

def parse_benchmark_file(filepath):
    pattern = re.compile(r'BenchmarkSegments/(\d+)MB_(\d+)KB_(BatchFlush|PerWriteFlush)-\d+\s+\d+\s+(\d+)\s+ns/op')
    results = {}

    with open(filepath, 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                seg_size_mb = int(match.group(1))
                chunk_size_kb = int(match.group(2))
                flush_mode = match.group(3)
                latency_ns = int(match.group(4))

                key = (seg_size_mb, flush_mode)
                results.setdefault(key, []).append((chunk_size_kb, latency_ns))

    return results

def plot_results(results):
    plt.figure(figsize=(10, 6))
    for (seg_size_mb, flush_mode), data in sorted(results.items()):
        data.sort()
        chunk_sizes = [chunk for chunk, _ in data]
        latencies_ms = [ns / 1e6 for _, ns in data]
        label = f"{seg_size_mb}MB - {flush_mode}"
        plt.plot(chunk_sizes, latencies_ms, marker='o', label=label)

    plt.xlabel("Chunk Size (KB)")
    plt.ylabel("Latency (ms)")
    plt.title("Write + Flush Latency vs Chunk Size")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python plot.py results.txt")
        sys.exit(1)

    filepath = sys.argv[1]
    parsed_data = parse_benchmark_file(filepath)
    plot_results(parsed_data)
