import re
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Define benchmark file paths
files = {
    "UnisonDB": "unisondb_benchmark.txt",
    "BadgerDB": "badger_benchmark.txt",
    "BoltDB": "boltdb_benchmark.txt"
}

def parse_set_benchmark(filepath):
    with open(filepath, "r") as f:
        data = f.read()
    set_p50 = [float(p) for p in re.findall(r'SET: [\d.]+ requests per second, p50=([\d.]+) msec', data)]
    set_tp  = [float(t) for t in re.findall(r'SET: ([\d.]+) requests per second, p50=[\d.]+ msec', data)]
    return set_tp, set_p50

# Color map per DB
colors = {
    "UnisonDB": "blue",
    "BadgerDB": "green",
    "BoltDB": "red"
}

plt.figure(figsize=(7, 5))
for db, path in files.items():
    set_tp, set_p50 = parse_set_benchmark(path)
    plt.scatter(set_tp, set_p50, label=db, alpha=0.7, color=colors[db])

plt.title("SET: Throughput vs p50 Latency")
plt.xlabel("Throughput (requests/sec)")
plt.ylabel("p50 Latency (ms)")
plt.grid(True, linestyle="--", linewidth=0.5)

ax = plt.gca()
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

plt.legend()
plt.tight_layout()
plt.show()
