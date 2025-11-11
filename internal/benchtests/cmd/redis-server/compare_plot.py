import csv
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Check if file arguments are provided
if len(sys.argv) < 4:
    print("Usage: python compare_plot.py <unisondb_csv> <badgerdb_csv> <boltdb_csv>")
    sys.exit(1)

# Define benchmark file paths
files = {
    "UnisonDB": sys.argv[1],
    "BadgerDB": sys.argv[2],
    "BoltDB": sys.argv[3]
}

def parse_set_benchmark(filepath):
    set_p50 = []
    set_tp = []

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip header rows or test rows
            if row['command'] == 'test':
                continue

            # Only parse SET commands
            if row['command'] == 'SET':
                try:
                    set_tp.append(float(row['requests_per_sec']))
                    set_p50.append(float(row['p50_ms']))
                except ValueError:
                    # Skip rows with non-numeric values
                    continue

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
