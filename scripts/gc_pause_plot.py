import pandas as pd
import matplotlib.pyplot as plt
import argparse

parser = argparse.ArgumentParser(description="Plot Go GC Pause Times Over Time")
parser.add_argument("file", type=str, help="Path to GC pause data file (gc_pause.csv)")
args = parser.parse_args()

df = pd.read_csv(args.file, sep=" ", names=["Time", "PauseTime"])

plt.figure(figsize=(10, 5))
plt.plot(df["Time"], df["PauseTime"], label="GC Pause Time (ms)", marker="o", color="r")

# Labels & title
plt.xlabel("Time (s)")
plt.ylabel("Pause Duration (ms)")
plt.title("Go GC Pause Times Over Time")
plt.legend()
plt.grid()
plt.show()