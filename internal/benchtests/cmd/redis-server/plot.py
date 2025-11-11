import csv
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Check if file argument is provided
if len(sys.argv) < 2:
    print("Usage: python plot.py <csv_file>")
    sys.exit(1)

csv_file = sys.argv[1]

# Read CSV data
set_p50 = []
get_p50 = []
set_throughput = []
get_throughput = []

with open(csv_file, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Skip header rows or test rows
        if row['command'] == 'test':
            continue

        try:
            rps = float(row['requests_per_sec'])
            latency = float(row['p50_ms'])

            if row['command'] == 'SET':
                set_throughput.append(rps)
                set_p50.append(latency)
            elif row['command'] == 'GET':
                get_throughput.append(rps)
                get_p50.append(latency)
        except ValueError:
            # Skip rows with non-numeric values
            continue


# Plot throughput vs p50 scatter
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.scatter(set_throughput, set_p50, alpha=0.7, color='blue')
plt.title('SET: Throughput vs p50 Latency')
plt.xlabel('Throughput (req/s)')
plt.ylabel('p50 Latency (ms)')
plt.grid(True)
ax = plt.gca()
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

plt.subplot(1, 2, 2)
plt.scatter(get_throughput, get_p50, alpha=0.7, color='green')
plt.title('GET: Throughput vs p50 Latency')
plt.xlabel('Throughput (req/s)')
plt.ylabel('p50 Latency (ms)')
plt.grid(True)
ax = plt.gca()
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

plt.tight_layout()
plt.show()
