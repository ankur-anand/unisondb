import sys
import re
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
                access, seg, chunk, mode, latency = match.groups()
                chunk_kb = int(chunk)
                latency_ns = int(latency)
                key = f"{access}/{mode}"
                results[key].append((chunk_kb, latency_ns))

    return results

def summarize(results):
    small_chunks = defaultdict(list)
    large_chunks = defaultdict(list)

    for mode, entries in results.items():
        for chunk_kb, latency_ns in entries:
            if chunk_kb <= 16:
                small_chunks[mode].append(latency_ns)
            else:
                large_chunks[mode].append(latency_ns)

    def print_summary(title, group):
        print(f"\n### {title}")
        print(f"{'| Mode':<26}| Avg Latency (ms)")
        print("-" * 42)
        for mode, latencies in sorted(group.items()):
            avg_latency_ms = sum(latencies) / len(latencies) / 1e6
            print(f"{mode:<26}| {avg_latency_ms:>12.2f} ms")

    print_summary("Small Chunks (≤16KB)", small_chunks)
    print_summary("Large Chunks (>16KB)", large_chunks)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python summary.py <benchmark_results.txt>")
        sys.exit(1)

    results = parse_benchmark_file(sys.argv[1])
    summarize(results)
