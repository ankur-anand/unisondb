import pandas as pd
import matplotlib.pyplot as plt
import os

csv_file = "benchmark_results.csv"
if not os.path.exists(csv_file):
    print(f"Error: {csv_file} not found. Run the Go benchmark script first.")
    exit(1)

df = pd.read_csv(csv_file)
df = df[df["Method"] != "Method"]

df["Avg Time Per Iteration (ms)"] = df["Avg Time Per Iteration (ms)"].astype(float)
df["goroutines"] = df["goroutines"].astype(int)

pivot_df = df.pivot(index="goroutines", columns="Method", values="Avg Time Per Iteration (ms)")

plt.figure(figsize=(10, 6))
pivot_df.plot(kind="line", marker="o", figsize=(10, 6))

plt.xlabel("Number of Goroutines")
plt.ylabel("Avg Time Per Iteration (ms)")
plt.title("Benchmark Performance: sync.Cond vs Buffered Channel vs Unbuffered Channel")
plt.legend(title="Method")

plt.grid(True)
plt.show()

