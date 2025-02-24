import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("./benchmark_results.csv")

df["Size"] = df["Size"].astype(int)

time_columns = [
    "random",
    "sorted",
    "batch_sorted",
    "batch_random"
]
df[time_columns] = df[time_columns] * 1000


plt.figure(figsize=(10, 5))
for method in time_columns:
    subset = df[df["Page Size"] == 4096]
    plt.plot(subset["Size"], subset[method], marker="o", label=method)

plt.xlabel("Number of Insertions")
plt.ylabel("Time (milliseconds)")
plt.title("BoltDB Insert Benchmark Results")
plt.legend()
plt.grid(True)
plt.xscale("log")

plot_filename = "boltdb_benchmark_results.png"
plt.savefig(plot_filename, dpi=300, bbox_inches="tight")

plt.show()

print(f"Plot saved as {plot_filename}")


