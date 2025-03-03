import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse

parser = argparse.ArgumentParser(description="Parse Go benchmark results from a file.")
parser.add_argument("filename", type=str, help="Path to the benchmark results file")
args = parser.parse_args()

def parse_benchmark_results(benchmark_text):
    data = []

    for line in benchmark_text.split("\n"):

        if not line.strip():
            continue

        # Only process lines that start with "Benchmark"
        if line.startswith("Benchmark"):
            parts = line.split()

            benchmark = parts[0].replace("Benchmark", "").replace("-10", "")
            benchmark = benchmark.split("/")[-1]
            iterations = int(parts[1])
            time_ns = float(parts[2])
            memory_b = int(parts[4])
            allocs = int(parts[6])

            data.append([benchmark, iterations, time_ns, memory_b, allocs])

    return pd.DataFrame(data, columns=["Benchmark", "Iterations", "Time (ns/op)", "Memory (B/op)", "Allocations (ops)"])

def plot_metric(df, metric, title, xlabel, colormap):
    # df_avg = df.groupby("Benchmark", as_index=False).mean()
    df[metric] = df[metric].rank(pct=True) * 100
    colors = colormap(np.linspace(0, 1, len(df)))

    plt.figure(figsize=(8, 5))
    plt.barh(df["Benchmark"], df[metric], color=colors)
    plt.xlabel(f"Percentile Rank of {xlabel} (Lower is Better)")
    plt.ylabel("Benchmark")
    plt.title(f"{title} (Percentile Rank)")
    plt.gca().invert_yaxis()
    plt.grid(axis="x", linestyle="--", alpha=0.6)  # Adjust grid to x-axis
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    with open(args.filename, "r") as file:
        bench_txt = file.read()

    df = parse_benchmark_results(bench_txt)
    print(df.to_string(index=False))

    plot_metric(df, "Time (ns/op)", "Time per Operation", "Time (ns/op)", plt.cm.viridis)
    plot_metric(df, "Memory (B/op)", "Memory Usage per Operation", "Memory (B/op)", plt.cm.plasma)
