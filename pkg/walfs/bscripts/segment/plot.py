import sys
import re
import matplotlib.pyplot as plt
from collections import defaultdict

def parse_benchmark_file(filepath):
    pattern = re.compile(
        r'BenchmarkSegments/(MMAP|FD)_(\d+)MB_(\d+)KB_(BatchFlush|PerWriteFlush|BatchSync|PerWriteSync)-\d+\s+\d+\s+(\d+)\s+ns/op'
    )
    results = defaultdict(list)

    with open(filepath, 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                io_type = match.group(1)
                seg_size_mb = int(match.group(2))
                chunk_size_kb = int(match.group(3))
                mode = match.group(4)
                latency_ns = int(match.group(5))

                key = (seg_size_mb, chunk_size_kb, io_type, mode)
                results[key].append(latency_ns)

    return results

def print_and_plot_results(results):
    grouped = defaultdict(list)

    for (seg_size, chunk_size, io_type, mode), latencies in sorted(results.items()):
        avg_ns = sum(latencies) / len(latencies)
        grouped[(seg_size, mode, io_type)].append((chunk_size, avg_ns))

    plt.figure(figsize=(12, 6))

    for (seg_size, mode, io_type), data in sorted(grouped.items()):
        data.sort()
        chunk_sizes = [c for c, _ in data]
        latencies_ms = [l / 1e6 for _, l in data]
        label = f"{io_type} {seg_size}MB {mode}"
        print(f"\n{label}")
        for c, l in zip(chunk_sizes, latencies_ms):
            print(f"  Chunk Size: {c}KB -> Latency: {l:.3f} ms")
        plt.plot(chunk_sizes, latencies_ms, marker='o', label=label)

    plt.xlabel("Chunk Size (KB)")
    plt.ylabel("Latency (ms)")
    plt.title("Write Latency vs Chunk Size")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python plot.py results.txt")
        sys.exit(1)

    filepath = sys.argv[1]
    parsed = parse_benchmark_file(filepath)
    print_and_plot_results(parsed)
    