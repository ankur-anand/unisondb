import pandas as pd
import matplotlib.pyplot as plt
import argparse

parser = argparse.ArgumentParser(description="Plot Go GC Heap Usage Over Time")
parser.add_argument("file", type=str, help="Path to GC log data file (gc_data.csv)")
args = parser.parse_args()


df = pd.read_csv(args.file, sep=" ", names=["Time", "HeapBefore", "HeapAfter"])

plt.figure(figsize=(10, 5))
plt.plot(df["Time"], df["HeapBefore"], label="Heap Before GC", marker="o", linestyle="--")
plt.plot(df["Time"], df["HeapAfter"], label="Heap After GC", marker="x", linestyle="-")

plt.xlabel("Time (s)")
plt.ylabel("Heap Size (MB)")
plt.title("Go GC Heap Usage Over Time")
plt.legend()
plt.grid()
plt.show()
