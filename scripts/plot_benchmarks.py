import argparse
import json
import os
import re
import pandas as pd
import matplotlib.pyplot as plt

plt.style.use("seaborn-v0_8-whitegrid")


def parse_benchmark_name(name):
    name_parts = name.split("-")
    base_name = name_parts[0]

    parts = base_name.split("/")
    data = {}
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            data[key] = value

    data["Benchmark"] = parts[0]

    if "Workers" in data:
        data["Workers"] = int(data["Workers"])
    if "Submitters" in data:
        data["Submitters"] = int(data["Submitters"])

    return data


def load_benchmark_data(filepath):
    records = []
    with open(filepath, "r") as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get("Action") == "output" and " ns/op" in record.get(
                    "Output", ""
                ):
                    output = record["Output"].strip()

                    full_name = output.split()[0]

                    ns_op_match = re.search(r"(\d+(\.\d+)?)\s+ns/op", output)
                    if ns_op_match:
                        parsed_name = parse_benchmark_name(full_name)
                        parsed_name["ns_op"] = float(ns_op_match.group(1))
                        records.append(parsed_name)
            except (json.JSONDecodeError, IndexError):
                continue
    return pd.DataFrame(records)


def plot_cpu_scaling(df, output_dir):
    print("Generating CPU-bound scaling plot...")

    median_submitters = df["Submitters"].median()
    cpu_df = df[
        (df["Task"] == "CPU") & (df["Submitters"] == median_submitters)
    ].sort_values("Workers")

    if cpu_df.empty:
        print("No data found for CPU scaling plot. Skipping.")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(cpu_df["Workers"], cpu_df["ns_op"], marker="o", linestyle="-")

    plt.title("CPU-Bound Task Performance Scaling", fontsize=16)
    plt.xlabel("Number of Workers", fontsize=12)
    plt.ylabel("Time per Operation (ns/op) - Lower is Better", fontsize=12)
    plt.xscale("log", base=2)
    plt.xticks(cpu_df["Workers"].unique(), labels=cpu_df["Workers"].unique())
    plt.grid(True, which="both", ls="--")

    filename = os.path.join(output_dir, "cpu_scaling.png")
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved plot to {filename}")


def plot_io_scaling(df, output_dir):
    print("Generating I/O-bound scaling plot...")

    median_submitters = df["Submitters"].median()
    io_df = df[
        (df["Task"] == "IO") & (df["Submitters"] == median_submitters)
    ].sort_values("Workers")

    if io_df.empty:
        print("No data found for I/O scaling plot. Skipping.")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(io_df["Workers"], io_df["ns_op"], marker="o", linestyle="-")

    plt.title("I/O-Bound Task Throughput Scaling", fontsize=16)
    plt.xlabel("Number of Workers", fontsize=12)
    plt.ylabel("Time per Operation (ns/op) - Lower is Better", fontsize=12)
    plt.yscale("log")
    plt.xscale("log", base=2)
    plt.xticks(io_df["Workers"].unique(), labels=io_df["Workers"].unique())
    plt.grid(True, which="both", ls="--")

    filename = os.path.join(output_dir, "io_scaling.png")
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved plot to {filename}")


def plot_overhead_comparison(df, output_dir):
    print("Generating overhead comparison plot...")

    pool_df = df[(df["Benchmark"] == "BenchmarkSubmit") & (df["Task"] == "Trivial")]
    pool_overhead = pool_df["ns_op"].min() if not pool_df.empty else float("nan")

    goroutine_df = df[
        (df["Benchmark"] == "BenchmarkGoroutinePerTask") & (df["Task"] == "Trivial")
    ]
    goroutine_overhead = (
        goroutine_df["ns_op"].mean() if not goroutine_df.empty else float("nan")
    )

    if pd.isna(pool_overhead) or pd.isna(goroutine_overhead):
        print("Not enough data for overhead comparison. Skipping.")
        return

    labels = ["Worker Pool (Best Case)", "Goroutine per Task"]
    values = [pool_overhead, goroutine_overhead]

    plt.figure(figsize=(8, 6))
    bars = plt.bar(labels, values, color=["#4C72B0", "#C44E52"])
    plt.bar_label(bars, fmt="{:,.1f} ns")

    plt.title("Submission Overhead Comparison (Trivial Task)", fontsize=16)
    plt.ylabel("Time per Operation (ns/op)", fontsize=12)
    plt.xticks(rotation=10)

    filename = os.path.join(output_dir, "overhead_comparison.png")
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved plot to {filename}")


def main():
    parser = argparse.ArgumentParser(description="Visualize Go benchmark results.")
    parser.add_argument(
        "--input", required=True, help="Path to the benchmark JSON output file."
    )
    parser.add_argument(
        "--output-dir", default="results/plots", help="Directory to save the plots."
    )
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    df = load_benchmark_data(args.input)

    if df.empty:
        print("No benchmark data loaded. Exiting.")
        return

    plot_cpu_scaling(df, args.output_dir)
    plot_io_scaling(df, args.output_dir)
    plot_overhead_comparison(df, args.output_dir)


if __name__ == "__main__":
    main()
