#!/usr/bin/env python3
"""
wikidata_analyze_rwikipedia.py
==============================

Loads the Kinyarwanda Wikipedia JSONL dump into a pandas DataFrame and
produces statistical analysis and visualisations of article characteristics.

Usage
-----
  python wikidata_analyze_rwikipedia.py

  python wikidata_analyze_rwikipedia.py \
      --input wikipedia/output/rw_0.jsonl \
      --out   wikipedia/output/analysis

Analyses produced
-----------------
  1. Word-count distribution (histogram + CDF)
  2. Character-count distribution
  3. Title-length distribution
  4. Top-20 longest articles
  5. Top-20 shortest articles (non-empty)
  6. Articles with zero content
  7. Summary statistics table
  8. Scatter plot: title length vs word count
  9. Log-scale histogram (to reveal long-tail structure)
 10. Cumulative content share (what fraction of total words is in the
     top N % of articles?)

All figures and tables are saved to the output directory.
"""

import argparse
import json
import os
import re
import sys

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_INPUT = "wikipedia/output/rw_0.jsonl"
DEFAULT_OUT = "wikipedia/output/analysis"


# ── helpers ───────────────────────────────────────────────────────────────────

_KW_RE = re.compile(r"[^\s]+")


def _word_count(text: str) -> int:
    return len(_KW_RE.findall(text))


def _load_jsonl(path: str) -> pd.DataFrame:
    records = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    df = pd.DataFrame(records)
    df["word_count"] = df["text"].apply(_word_count)
    df["char_count"] = df["text"].str.len()
    df["title_char_count"] = df["title"].str.len()
    df["title_word_count"] = df["title"].apply(_word_count)
    return df


# ── plotting helpers ─────────────────────────────────────────────────────────


def _save(fig, name: str, out_dir: str) -> None:
    fig.savefig(os.path.join(out_dir, name), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {name}")


def plot_word_hist(df: pd.DataFrame, out_dir: str) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # linear histogram (clipped at 2000 words for readability)
    clipped = df[df["word_count"] <= 2000]["word_count"]
    axes[0].hist(clipped, bins=100, edgecolor="black", color="#4C72B0")
    axes[0].set_xlabel("Word count")
    axes[0].set_ylabel("Number of articles")
    axes[0].set_title("Word-count distribution (≤ 2000 words)")
    axes[0].axvline(
        df["word_count"].median(),
        color="red",
        ls="--",
        label=f"median = {df['word_count'].median():.0f}",
    )
    axes[0].legend()

    # CDF
    sorted_wc = np.sort(df["word_count"])
    cdf = np.arange(1, len(sorted_wc) + 1) / len(sorted_wc)
    axes[1].plot(sorted_wc, cdf, color="#55A868", linewidth=2)
    axes[1].set_xlabel("Word count")
    axes[1].set_ylabel("Cumulative fraction")
    axes[1].set_title("Cumulative distribution function")
    axes[1].grid(True, alpha=0.3)

    _save(fig, "01_word_count_distribution.png", out_dir)


def plot_word_hist_log(df: pd.DataFrame, out_dir: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(df["word_count"], bins=100, edgecolor="black", color="#DD8452", log=True)
    ax.set_xlabel("Word count (log scale)")
    ax.set_ylabel("Number of articles (log scale)")
    ax.set_title("Word-count distribution (log-log)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3, which="both")
    _save(fig, "02_word_count_loglog.png", out_dir)


def plot_char_hist(df: pd.DataFrame, out_dir: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(df["char_count"], bins=100, edgecolor="black", color="#C44E52")
    ax.set_xlabel("Character count")
    ax.set_ylabel("Number of articles")
    ax.set_title("Character-count distribution")
    ax.axvline(
        df["char_count"].median(),
        color="red",
        ls="--",
        label=f"median = {df['char_count'].median():.0f}",
    )
    ax.legend()
    _save(fig, "03_char_count_distribution.png", out_dir)


def plot_title_length(df: pd.DataFrame, out_dir: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(df["title_char_count"], bins=50, edgecolor="black", color="#8172B3")
    ax.set_xlabel("Title length (characters)")
    ax.set_ylabel("Number of articles")
    ax.set_title("Title-length distribution")
    _save(fig, "04_title_length_distribution.png", out_dir)


def plot_scatter_title_vs_words(df: pd.DataFrame, out_dir: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(
        df["title_word_count"], df["word_count"], alpha=0.3, s=8, color="#4C72B0"
    )
    ax.set_xlabel("Title word count")
    ax.set_ylabel("Article word count")
    ax.set_title("Title length vs article length")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3)
    _save(fig, "05_title_vs_words_scatter.png", out_dir)


def plot_cumulative_share(df: pd.DataFrame, out_dir: str) -> None:
    """What fraction of total words is held by the top N% of articles?"""
    sorted_wc = np.sort(df["word_count"])[::-1]  # descending
    total = sorted_wc.sum()
    cumsum = np.cumsum(sorted_wc) / total
    pct = np.arange(1, len(sorted_wc) + 1) / len(sorted_wc) * 100

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(pct, cumsum * 100, color="#55A868", linewidth=2)
    ax.plot([0, 100], [0, 100], "k--", alpha=0.3, label="Uniform baseline")
    ax.set_xlabel("Top N% of articles")
    ax.set_ylabel("Share of total words (%)")
    ax.set_title("Cumulative word-share (Lorenz-style curve)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    _save(fig, "06_cumulative_word_share.png", out_dir)


# ── tables ───────────────────────────────────────────────────────────────────


def table_summary(df: pd.DataFrame, out_dir: str) -> None:
    col = "word_count"
    stats = {
        "Total articles": len(df),
        "Total words": df[col].sum(),
        "Mean words/article": df[col].mean(),
        "Median words/article": df[col].median(),
        "Std deviation": df[col].std(),
        "Min": df[col].min(),
        "Max": df[col].max(),
        "P10": df[col].quantile(0.10),
        "P25": df[col].quantile(0.25),
        "P75": df[col].quantile(0.75),
        "P90": df[col].quantile(0.90),
        "P95": df[col].quantile(0.95),
        "P99": df[col].quantile(0.99),
        "Articles with 0 words": (df[col] == 0).sum(),
    }
    tbl = pd.DataFrame.from_dict(stats, orient="index", columns=["value"])
    tbl.index.name = "metric"
    tbl.to_csv(os.path.join(out_dir, "07_summary_stats.csv"))
    print(f"  saved 07_summary_stats.csv")
    print()
    for k, v in stats.items():
        if isinstance(v, float):
            print(f"  {k:30s} {v:,.1f}")
        else:
            print(f"  {k:30s} {v:,}")


def table_top(df: pd.DataFrame, out_dir: str, n: int = 20) -> None:
    top = df.nlargest(n, "word_count")[["id", "title", "word_count", "char_count"]]
    top.to_csv(os.path.join(out_dir, "08_top20_longest.csv"), index=False)
    print(f"  saved 08_top20_longest.csv")

    short = df[df["word_count"] > 0].nsmallest(n, "word_count")[
        ["id", "title", "word_count", "char_count"]
    ]
    short.to_csv(os.path.join(out_dir, "09_top20_shortest.csv"), index=False)
    print(f"  saved 09_top20_shortest.csv")

    empty = df[df["word_count"] == 0][["id", "title"]]
    empty.to_csv(os.path.join(out_dir, "10_empty_articles.csv"), index=False)
    print(f"  saved 10_empty_articles.csv ({len(empty)} empty articles)")


# ── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyse Kinyarwanda Wikipedia article lengths."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to the JSONL file")
    parser.add_argument(
        "--out", default=DEFAULT_OUT, help="Output directory for figures and tables"
    )
    parser.add_argument(
        "--top", type=int, default=20, help="Number of entries in top/shortest tables"
    )
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print(f"Loading {args.input} …")
    df = _load_jsonl(args.input)
    print(f"  {len(df):,} articles loaded\n")

    print("Generating plots …")
    plot_word_hist(df, args.out)
    plot_word_hist_log(df, args.out)
    plot_char_hist(df, args.out)
    plot_title_length(df, args.out)
    plot_scatter_title_vs_words(df, args.out)
    plot_cumulative_share(df, args.out)

    print("\nGenerating tables …")
    table_summary(df, args.out)
    table_top(df, args.out, n=args.top)

    print(f"\nAll outputs saved to {args.out}/")


if __name__ == "__main__":
    main()
