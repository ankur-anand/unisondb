#!/usr/bin/env python3
import argparse
import math
import re
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt
import pandas as pd


SET_LINE = re.compile(
    r'^\s*[^,]+?,\s*(?P<run>\d+),"SET","(?P<rps>[\d.]+)","(?P<lat>[\d.]+)"'
)
GET_LINE = re.compile(
    r'^\s*[^,]+?,\s*(?P<run>\d+),"GET","(?P<rps>[\d.]+)","(?P<lat>[\d.]+)"'
)
TOTAL_RECORDS = re.compile(r"Total Records:\s*(?P<total>\d+)")
THROUGHPUT = re.compile(r"Throughput\s*:\s*(?P<rps>[\d.]+)")
REPL_LAT = re.compile(
    r"Latency \(ms\)\s*:\s*p50=(?P<p50>[\d.]+)\s+p90=(?P<p90>[\d.]+)\s+p99=(?P<p99>[\d.]+)"
)


def parse_blocks(log_path: Path):
    blocks = []
    current = None

    with log_path.open() as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if line.startswith("###"):
                if current:
                    blocks.append(current)
                tokens = line.split()
                relayer_token = next(
                    (tok for tok in tokens if tok.startswith("RELAYER_COUNT=")), None
                )
                if not relayer_token:
                    continue
                relayers = int(relayer_token.split("=", 1)[1])
                current = {
                    "relayers": relayers,
                    "set_samples": [],
                    "get_samples": [],
                    "replication": {},
                }
                continue

            if not current:
                continue

            if match := SET_LINE.match(line):
                current["set_samples"].append(
                    (float(match.group("rps")), float(match.group("lat")))
                )
                continue

            if match := GET_LINE.match(line):
                current["get_samples"].append(
                    (float(match.group("rps")), float(match.group("lat")))
                )
                continue

            if match := TOTAL_RECORDS.search(line):
                current["replication"]["total_records"] = float(match.group("total"))
                continue

            if match := THROUGHPUT.search(line):
                current["replication"]["throughput"] = float(match.group("rps"))
                continue

            if match := REPL_LAT.search(line):
                current["replication"]["p50"] = float(match.group("p50"))
                current["replication"]["p90"] = float(match.group("p90"))
                current["replication"]["p99"] = float(match.group("p99"))
                continue

    if current:
        blocks.append(current)
    return blocks


def summarize(blocks):
    rows = []
    for block in blocks:
        row = {"relayers": block["relayers"]}

        if block["set_samples"]:
            row["set_rps"] = mean(r for r, _ in block["set_samples"])
            row["set_latency_ms"] = mean(lat for _, lat in block["set_samples"])
        else:
            row["set_rps"] = math.nan
            row["set_latency_ms"] = math.nan

        if block["get_samples"]:
            row["get_rps"] = mean(r for r, _ in block["get_samples"])
            row["get_latency_ms"] = mean(lat for _, lat in block["get_samples"])
        else:
            row["get_rps"] = math.nan
            row["get_latency_ms"] = math.nan

        repl = block["replication"]
        row["replication_throughput"] = repl.get("throughput", math.nan)
        row["replication_p50"] = repl.get("p50", math.nan)
        row["replication_p90"] = repl.get("p90", math.nan)
        row["replication_p99"] = repl.get("p99", math.nan)
        row["replication_total_records"] = repl.get("total_records", math.nan)

        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No relayer blocks detected in the log.")
    return df.sort_values("relayers").reset_index(drop=True)


def plot_latency(df, output_dir: Path):
    fig = plt.figure(figsize=(12, 6), layout="constrained")
    fig.set_constrained_layout_pads(w_pad=0.1, h_pad=0.45, hspace=0.25, wspace=0.2)
    spec = fig.add_gridspec(2, 2, height_ratios=[1, 1.1])

    ax_set = fig.add_subplot(spec[0, 0])
    ax_get = fig.add_subplot(spec[0, 1])
    ax_repl_zoom = fig.add_subplot(spec[1, 0])
    ax_repl_full = fig.add_subplot(spec[1, 1])

    ax_set.plot(df["relayers"], df["set_latency_ms"], marker="o")
    ax_set.set_title("SET avg latency")
    ax_set.set_ylabel("Latency (ms)")
    ax_set.grid(True, linestyle="--", alpha=0.4)

    ax_get.plot(df["relayers"], df["get_latency_ms"], marker="o", color="tab:orange")
    ax_get.set_title("GET avg latency")
    ax_get.grid(True, linestyle="--", alpha=0.4)
    ax_get.set_ylabel("Latency (ms)")

    percentile_series = {
        "p50": ("tab:blue", df["replication_p50"]),
        "p90": ("tab:orange", df["replication_p90"]),
        "p99": ("tab:green", df["replication_p99"]),
    }
    for ax in (ax_repl_zoom, ax_repl_full):
        for label, (color, series) in percentile_series.items():
            ax.plot(df["relayers"], series, marker="o", color=color, label=label)
        ax.grid(True, linestyle="--", alpha=0.4)

    ax_repl_zoom.set_title("Replication latency (0–200 ms)")
    ax_repl_zoom.set_ylabel("Latency (ms)")
    ax_repl_zoom.set_ylim(0, 200)

    ax_repl_full.set_title("Replication latency (full scale)")
    ax_repl_full.set_ylabel("Latency (ms)")
    handles, labels = ax_repl_full.get_legend_handles_labels()
    handle_map = {label: handle for handle, label in zip(handles, labels)}
    legend_order = ["p99", "p50", "p90"]
    legend_handles = [handle_map[label] for label in legend_order if label in handle_map]
    legend_labels = [label for label in legend_order if label in handle_map]
    if legend_handles:
        ax_repl_full.legend(
            legend_handles,
            legend_labels,
            loc="upper left",
            bbox_to_anchor=(1.02, 1.0),
            frameon=True,
            ncol=1,
            fontsize=9,
        )

    fig.suptitle("Latency vs Relayer Count", y=1.02)
    fig.supxlabel("Relayer count")
    output = output_dir / "relayer_latency.png"
    fig.savefig(output, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return output


def plot_throughput(df, output_dir: Path):
    fig = plt.figure(figsize=(11, 6), layout="constrained")
    fig.set_constrained_layout_pads(w_pad=0.1, h_pad=0.4, hspace=0.25, wspace=0.2)
    spec = fig.add_gridspec(2, 2, height_ratios=[1, 1.1])

    ax_set = fig.add_subplot(spec[0, 0])
    ax_get = fig.add_subplot(spec[0, 1])
    ax_repl = fig.add_subplot(spec[1, :])

    ax_set.plot(df["relayers"], df["set_rps"], marker="o")
    ax_set.set_title("SET throughput")
    ax_set.set_ylabel("Ops/sec")
    ax_set.grid(True, linestyle="--", alpha=0.4)

    ax_get.plot(df["relayers"], df["get_rps"], marker="o", color="tab:orange")
    ax_get.set_title("GET throughput")
    ax_get.grid(True, linestyle="--", alpha=0.4)
    ax_get.set_ylabel("Ops/sec")

    ax_repl.plot(
        df["relayers"],
        df["replication_throughput"],
        marker="o",
        label="Replication throughput",
    )
    ax_repl.set_ylabel("Ops/sec")
    ax_repl.set_title("Replication throughput")
    ax_repl.grid(True, linestyle="--", alpha=0.4)
    ax_repl.legend(loc="upper left")

    fig.suptitle("Throughput vs Relayer Count", y=1.02)
    fig.supxlabel("Relayer count")
    output = output_dir / "relayer_throughput.png"
    fig.savefig(output, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return output


def plot_replication_latency_granular(df, output_dir: Path):
    fig, ax = plt.subplots(figsize=(8, 4), layout="constrained")
    fig.set_constrained_layout_pads(w_pad=0.1, h_pad=0.3, hspace=0.2, wspace=0.2)

    percentile_series = {
        "p50": ("tab:blue", df["replication_p50"]),
        "p90": ("tab:orange", df["replication_p90"]),
        "p99": ("tab:green", df["replication_p99"]),
    }
    for label, (color, series) in percentile_series.items():
        ax.plot(df["relayers"], series, marker="o", color=color, label=label)

    max_latency = 0.0
    for _, (_, series) in percentile_series.items():
        if hasattr(series, "max"):
            series_max = series.max(skipna=True)
            if math.isfinite(series_max):
                max_latency = max(max_latency, series_max)
    step = 100
    upper = max(step, int(math.ceil(max_latency / step)) * step)
    ax.set_yticks(range(0, upper + step, step))
    ax.set_ylim(0, upper)

    ax.set_title("Replication latency (100 ms granularity)")
    ax.set_xlabel("Relayer count")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(loc="upper left")

    output = output_dir / "relayer_latency_granular.png"
    fig.savefig(output, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return output


def plot_set_get_panels(df, output_dir: Path):
    fig, (ax_set, ax_get) = plt.subplots(1, 2, figsize=(10, 4), layout="constrained", sharex=True)

    # SET panel
    ax_set.plot(df["relayers"], df["set_latency_ms"], marker="o", color="tab:blue", label="Latency (ms)")
    ax_set.set_title("SET metrics")
    ax_set.set_ylabel("Latency (ms)", color="tab:blue")
    ax_set.tick_params(axis="y", labelcolor="tab:blue")
    ax_set.grid(True, linestyle="--", alpha=0.3)

    ax_set_t = ax_set.twinx()
    ax_set_t.plot(df["relayers"], df["set_rps"], marker="s", color="tab:orange", label="Throughput (ops/sec)")
    ax_set_t.set_ylabel("Throughput (ops/sec)", color="tab:orange")
    ax_set_t.tick_params(axis="y", labelcolor="tab:orange")

    # GET panel
    ax_get.plot(df["relayers"], df["get_latency_ms"], marker="o", color="tab:blue", label="Latency (ms)")
    ax_get.set_title("GET metrics")
    ax_get.grid(True, linestyle="--", alpha=0.3)
    ax_get.set_ylabel("Latency (ms)", color="tab:blue")
    ax_get.tick_params(axis="y", labelcolor="tab:blue")

    ax_get_t = ax_get.twinx()
    ax_get_t.plot(df["relayers"], df["get_rps"], marker="s", color="tab:orange", label="Throughput (ops/sec)")
    ax_get_t.set_ylabel("Throughput (ops/sec)", color="tab:orange")
    ax_get_t.tick_params(axis="y", labelcolor="tab:orange")

    ax_get.set_xlabel("Relayer count")
    ax_set.set_xlabel("Relayer count")

    fig.suptitle("SET / GET latency & throughput", y=1.02)
    output = output_dir / "relayer_set_get.png"
    fig.savefig(output, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Parse relayer benchmark log and plot latency/throughput vs relayer count."
    )
    parser.add_argument("logfile", type=Path, help="Path to the aggregated log file")
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("benchmarks"),
        help="Directory to write plots (default: benchmarks)",
    )
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)

    blocks = parse_blocks(args.logfile)
    df = summarize(blocks)
    print(df.to_string(index=False))

    latency_path = plot_latency(df, args.outdir)
    throughput_path = plot_throughput(df, args.outdir)
    granular_path = plot_replication_latency_granular(df, args.outdir)
    set_get_path = plot_set_get_panels(df, args.outdir)
    print(
        "\nSaved plots:\n"
        f"  {latency_path}\n"
        f"  {throughput_path}\n"
        f"  {granular_path}\n"
        f"  {set_get_path}"
    )


if __name__ == "__main__":
    main()
