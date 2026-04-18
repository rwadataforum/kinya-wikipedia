#!/usr/bin/env python3
"""
wikipedia_analyze_categories.py
================================
Category-level analysis of Kinyarwanda Wikipedia articles using the
rw_wikipedia_wikidata_analysis SQLite table.

Outputs
-------
  01_category_stats_top100.csv   — descriptive stats per category (top 100 by article count)
  02_articles_per_category.png   — horizontal bar chart, top 50 categories
  03_wordcount_spread.png        — box plot of word counts per category, top 50

Usage
-----
  python wikipedia_analyze_categories.py
"""

import os
import sqlite3

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

DB_FILE = "/home/xmikelinux/__ProjectsGitHub/kinya-wikipedia/output/analysis/analysis_db.sqlite"
OUT_DIR = "/home/xmikelinux/__ProjectsGitHub/kinya-wikipedia/03_analysis"


def load_data(db_file: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query("SELECT * FROM rw_wikipedia_wikidata_analysis", conn)
    conn.close()
    df = df[df["xobject"].notna() & (df["xobject"].str.strip() != "")]
    df["word_count_kin"] = pd.to_numeric(df["word_count_kin"], errors="coerce").fillna(0)
    return df


def build_category_stats(df: pd.DataFrame, top_n: int = 100) -> pd.DataFrame:
    grp = df.groupby("xobject")["word_count_kin"]
    stats = grp.agg(
        article_count="count",
        total_words="sum",
        mean_words="mean",
        median_words="median",
        std_words="std",
        min_words="min",
        max_words="max",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        p90=lambda x: x.quantile(0.90),
    ).reset_index()
    stats = stats.sort_values("article_count", ascending=False).head(top_n)
    stats = stats.round(1)
    stats.columns = [
        "category", "article_count", "total_words", "mean_words",
        "median_words", "std_words", "min_words", "max_words", "p25", "p75", "p90",
    ]
    return stats


def plot_articles_per_category(stats: pd.DataFrame, out_dir: str, top_n: int = 50) -> None:
    data = stats.head(top_n).sort_values("article_count", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 14))
    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(data)))
    bars = ax.barh(data["category"], data["article_count"], color=colors, edgecolor="white", linewidth=0.4)

    for bar, val in zip(bars, data["article_count"]):
        ax.text(bar.get_width() + data["article_count"].max() * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{int(val):,}", va="center", ha="left", fontsize=7.5)

    ax.set_xlabel("Number of articles", fontsize=11)
    ax.set_title(f"Kinyarwanda Wikipedia — Articles per category (top {top_n})", fontsize=13, pad=14)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=8)
    plt.tight_layout()

    path = os.path.join(out_dir, "02_articles_per_category.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


def plot_wordcount_spread(df: pd.DataFrame, stats: pd.DataFrame, out_dir: str, top_n: int = 50) -> None:
    top_cats = stats.head(top_n)["category"].tolist()
    subset = df[df["xobject"].isin(top_cats)].copy()

    # Order categories by median word count descending
    order = (
        subset.groupby("xobject")["word_count_kin"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )

    groups = [subset.loc[subset["xobject"] == cat, "word_count_kin"].values for cat in order]

    fig, ax = plt.subplots(figsize=(10, 14))

    bp = ax.boxplot(
        groups,
        vert=False,
        patch_artist=True,
        showfliers=True,
        flierprops=dict(marker="o", markersize=2, alpha=0.3, linestyle="none"),
        medianprops=dict(color="#c0392b", linewidth=1.5),
        boxprops=dict(facecolor="#aec6e8", alpha=0.8),
        whiskerprops=dict(linewidth=0.8),
        capprops=dict(linewidth=0.8),
    )

    ax.set_yticks(range(1, len(order) + 1))
    ax.set_yticklabels(order, fontsize=8)
    ax.set_xlabel("Word count per article", fontsize=11)
    ax.set_title(
        f"Kinyarwanda Wikipedia — Word count spread per category (top {top_n} by article count)",
        fontsize=12, pad=14,
    )
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()

    path = os.path.join(out_dir, "03_wordcount_spread.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


def main() -> None:
    print(f"Loading data from {DB_FILE} …")
    df = load_data(DB_FILE)
    print(f"  {len(df):,} articles with a category label\n")

    print("Building category stats (top 100) …")
    stats = build_category_stats(df, top_n=100)
    csv_path = os.path.join(OUT_DIR, "01_category_stats_top100.csv")
    stats.to_csv(csv_path, index=False)
    print(f"  saved {csv_path}")
    print(stats.head(20).to_string(index=False))
    print()

    print("Plotting articles per category (top 50) …")
    plot_articles_per_category(stats, OUT_DIR, top_n=50)

    print("Plotting word-count spread per category (top 50) …")
    plot_wordcount_spread(df, stats, OUT_DIR, top_n=50)

    print("\nDone.")


if __name__ == "__main__":
    main()
