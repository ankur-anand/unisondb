import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd


# The parser expects logs produced with GODEBUG=gctrace=1;
# python3 plot_gc_log.py gc_log.txt [--save out.png]
GC_RE = re.compile(
    r"gc\s+(?P<cycle>\d+)\s+@(?P<time>[0-9.]+)s\s+(?P<cpu_pct>\d+)%:"
    r"\s+(?P<clock>[^,]+) ms clock, (?P<cpu>[^,]+) ms cpu,"
    r"\s+(?P<heap>[0-9.]+->[0-9.]+->[0-9.]+) MB,"
    r"\s+(?P<goal>[0-9.]+) MB goal,"
    r"\s+(?P<stacks>[0-9.]+) MB stacks,"
    r"\s+(?P<globals>[0-9.]+) MB globals,"
    r"\s+(?P<procs>\d+) P"
)


def parse_gc_log(path: Path) -> pd.DataFrame:
    """Parse Go GC debug log produced with GODEBUG=gctrace=1."""
    rows: List[Dict[str, float]] = []
    for line in path.read_text().splitlines():
        m = GC_RE.match(line.strip())
        if not m:
            continue

        clock_parts = [float(part) for part in m.group("clock").split("+")]
        if len(clock_parts) == 3:
            stw_ms = clock_parts[0] + clock_parts[2]
            concurrent_ms = clock_parts[1]
        else:
            # Older Go versions only printed two sections.
            stw_ms = sum(clock_parts)
            concurrent_ms = 0.0

        heap_parts = [float(part) for part in m.group("heap").split("->")]

        rows.append(
            {
                "cycle": int(m.group("cycle")),
                "time_s": float(m.group("time")),
                "stw_ms": stw_ms,
                "concurrent_ms": concurrent_ms,
                "heap_start_mb": heap_parts[0],
                "heap_end_mb": heap_parts[1],
                "heap_live_mb": heap_parts[2],
                "heap_goal_mb": float(m.group("goal")),
                "cpu_pct": int(m.group("cpu_pct")),
                "procs": int(m.group("procs")),
            }
        )

    if not rows:
        raise ValueError(
            f"Did not find any GC trace lines in {path}. "
            "Ensure the log was collected with GODEBUG=gctrace=1."
        )

    df = pd.DataFrame(rows)
    return df.sort_values("cycle").reset_index(drop=True)


def plot_metrics(df: pd.DataFrame, save_path: Optional[Path]) -> None:
    fig, (ax_pause, ax_heap) = plt.subplots(
        2, 1, figsize=(12, 10), sharex=True, constrained_layout=True
    )

    ax_pause.plot(df["time_s"], df["stw_ms"], label="STW pause (ms)", color="tab:red")
    ax_pause.plot(
        df["time_s"],
        df["concurrent_ms"],
        label="Concurrent mark (ms)",
        color="tab:orange",
        alpha=0.7,
    )
    ax_pause.set_ylabel("Duration (ms)")
    ax_pause.set_title("Go GC Pauses")
    ax_pause.grid(True, alpha=0.3)
    ax_pause.legend()

    ax_heap.plot(
        df["time_s"],
        df["heap_start_mb"],
        label="Heap start",
        linestyle="--",
        color="tab:blue",
    )
    ax_heap.plot(
        df["time_s"],
        df["heap_live_mb"],
        label="Live heap",
        linestyle="-",
        color="tab:green",
    )
    ax_heap.plot(
        df["time_s"],
        df["heap_goal_mb"],
        label="Heap goal",
        linestyle=":",
        color="tab:purple",
    )
    ax_heap.set_xlabel("Time since process start (s)")
    ax_heap.set_ylabel("Megabytes")
    ax_heap.set_title("Go GC Heap Sizes")
    ax_heap.grid(True, alpha=0.3)
    ax_heap.legend()
    # ax_cpu.plot(
    #     df["time_s"],
    #     df["cpu_pct"],
    #     label="GC-induced mutator idle %",
    #     color="tab:brown",
    # )
    # ax_cpu.set_xlabel("Time since process start (s)")
    # ax_cpu.set_ylabel("Percent")
    # ax_cpu.set_title("GC CPU Throttling")
    # ax_cpu.set_ylim(bottom=0)
    # ax_cpu.grid(True, alpha=0.3)
    # ax_cpu.legend()

    if save_path:
        fig.savefig(save_path, dpi=200)
        print(f"Wrote {save_path}")
    else:
        plt.show()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot GC pause and heap metrics from a Go gc log"
    )
    parser.add_argument("logfile", type=Path, help="Path to gc_log.txt")
    parser.add_argument(
        "--save",
        type=Path,
        metavar="PNG",
        help="Optional path to save the figure instead of opening a window",
    )
    args = parser.parse_args()

    df = parse_gc_log(args.logfile)

    print(
        "Parsed {cycles} GCs covering {duration:.2f}s "
        "(avg pause {avg_pause:.3f} ms, p95 {p95_pause:.3f} ms)".format(
            cycles=len(df),
            duration=df["time_s"].iloc[-1] - df["time_s"].iloc[0],
            avg_pause=df["stw_ms"].mean(),
            p95_pause=df["stw_ms"].quantile(0.95),
        )
    )

    plot_metrics(df, args.save)


if __name__ == "__main__":
    main()

