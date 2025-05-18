import re
import matplotlib.pyplot as plt

with open("benchmark_output.txt", "r") as f:
    data = f.read()

# Extract values using regex
set_p50 = [float(p) for p in re.findall(r'SET: [\d.]+ requests per second, p50=([\d.]+) msec', data)]
get_p50 = [float(p) for p in re.findall(r'GET: [\d.]+ requests per second, p50=([\d.]+) msec', data)]
set_throughput = [float(t) for t in re.findall(r'SET: ([\d.]+) requests per second, p50=[\d.]+ msec', data)]
get_throughput = [float(t) for t in re.findall(r'GET: ([\d.]+) requests per second, p50=[\d.]+ msec', data)]


# Plot throughput vs p50 scatter
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.scatter(set_throughput, set_p50, alpha=0.7, color='blue')
plt.title('SET: Throughput vs p50 Latency')
plt.xlabel('Throughput (req/s)')
plt.ylabel('p50 Latency (ms)')
plt.grid(True)

plt.subplot(1, 2, 2)
plt.scatter(get_throughput, get_p50, alpha=0.7, color='green')
plt.title('GET: Throughput vs p50 Latency')
plt.xlabel('Throughput (req/s)')
plt.ylabel('p50 Latency (ms)')
plt.grid(True)

plt.tight_layout()
plt.show()
