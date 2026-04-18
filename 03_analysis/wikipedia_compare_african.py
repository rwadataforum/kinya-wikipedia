#!/usr/bin/env python3
"""
wikipedia_compare_african.py
=============================
Compares Kinyarwanda Wikipedia to other African-language Wikipedias
using the wikipedia_languages_dataset.xlsx dataset.

Outputs (saved to 03_analysis/)
--------------------------------
  04_african_wikis_articles.png     — bar chart: article counts, all African wikis
  05_african_wikis_articles_per_speaker.png — bar chart: articles per million speakers
  06_african_wikis_scatter.png      — scatter: speakers vs articles (log-log)
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

DATA_FILE = "/home/xmikelinux/__ProjectsGitHub/kinya-wikipedia/data/wikipedia_languages_dataset.xlsx"
OUT_DIR   = "/home/xmikelinux/__ProjectsGitHub/kinya-wikipedia/03_analysis"

HIGHLIGHT = "Kinyarwanda"
COL_TEAL  = "#2196A6"
COL_RED   = "#E02020"
BG        = "#F7F9FC"


def load_africa(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Wikipedia Language Stats")
    africa = df[df["Primary Continent(s)"].str.strip() == "Africa"].copy()
    africa = africa[~africa["Language"].isin(["French", "Arabic", "Portuguese"])]
    africa = africa.dropna(subset=["Total Speakers (M)", "Articles"])
    africa = africa[africa["Total Speakers (M)"] > 0]
    africa["articles_per_M_speakers"] = (
        africa["Articles"] / africa["Total Speakers (M)"]
    ).round(1)
    return africa.reset_index(drop=True)


def _bar_colors(languages, highlight=HIGHLIGHT):
    return [COL_RED if l == highlight else COL_TEAL for l in languages]


def plot_articles(africa: pd.DataFrame) -> None:
    data = africa.sort_values("Articles", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 12))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)

    colors = _bar_colors(data["Language"])
    bars = ax.barh(data["Language"], data["Articles"], color=colors, edgecolor="white", linewidth=0.3)

    for bar, val in zip(bars, data["Articles"]):
        ax.text(bar.get_width() + data["Articles"].max() * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{int(val):,}", va="center", ha="left", fontsize=7.5)

    ax.set_xlabel("Number of articles", fontsize=11)
    ax.set_title("African-language Wikipedias — Article count\n(excl. French, Arabic, Portuguese)", fontsize=12, pad=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=8)

    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=COL_RED, label=HIGHLIGHT), Patch(color=COL_TEAL, label="Other")],
              fontsize=9, framealpha=0.6)

    plt.tight_layout()
    path = os.path.join(OUT_DIR, "04_african_wikis_articles.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


def plot_articles_per_speaker(africa: pd.DataFrame) -> None:
    data = africa.sort_values("articles_per_M_speakers", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 12))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)

    colors = _bar_colors(data["Language"])
    bars = ax.barh(data["Language"], data["articles_per_M_speakers"], color=colors,
                   edgecolor="white", linewidth=0.3)

    for bar, val in zip(bars, data["articles_per_M_speakers"]):
        ax.text(bar.get_width() + data["articles_per_M_speakers"].max() * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{val:,.0f}", va="center", ha="left", fontsize=7.5)

    ax.set_xlabel("Articles per million speakers", fontsize=11)
    ax.set_title("African-language Wikipedias — Articles per million speakers\n(excl. French, Arabic, Portuguese)", fontsize=12, pad=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=8)

    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=COL_RED, label=HIGHLIGHT), Patch(color=COL_TEAL, label="Other")],
              fontsize=9, framealpha=0.6)

    plt.tight_layout()
    path = os.path.join(OUT_DIR, "05_african_wikis_articles_per_speaker.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


def plot_scatter(africa: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 6))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)

    other = africa[africa["Language"] != HIGHLIGHT]
    rw    = africa[africa["Language"] == HIGHLIGHT]

    ax.scatter(other["Total Speakers (M)"], other["Articles"],
               color=COL_TEAL, s=45, edgecolor="white", linewidth=0.4, alpha=0.85, zorder=3)
    ax.scatter(rw["Total Speakers (M)"], rw["Articles"],
               color=COL_RED, s=100, edgecolor="white", linewidth=0.7, zorder=5)

    # Label only notable languages to avoid clutter
    label_langs = {HIGHLIGHT, "Afrikaans", "Swahili", "Hausa", "Amharic",
                   "Yoruba", "Igbo", "Zulu", "Malagasy", "Egyptian Arabic", "Tumbuka"}
    for _, row in africa.iterrows():
        if row["Language"] not in label_langs:
            continue
        color  = COL_RED if row["Language"] == HIGHLIGHT else "#333333"
        weight = "bold"  if row["Language"] == HIGHLIGHT else "normal"
        ax.annotate(row["Language"],
                    xy=(row["Total Speakers (M)"], row["Articles"]),
                    xytext=(4, 2), textcoords="offset points",
                    fontsize=7.5, color=color, fontweight=weight, va="bottom")

    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("Total speakers (millions, log scale)", fontsize=11)
    ax.set_ylabel("Number of articles (log scale)", fontsize=11)
    ax.set_title("African-language Wikipedias — Speakers vs Articles (log-log)\n(excl. French, Arabic, Portuguese)",
                 fontsize=12, pad=12)
    ax.grid(True, which="both", alpha=0.2)
    ax.spines[["top", "right"]].set_visible(False)

    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=COL_RED, label=HIGHLIGHT), Patch(color=COL_TEAL, label="Other")],
              fontsize=9, framealpha=0.6)

    plt.tight_layout()
    path = os.path.join(OUT_DIR, "06_african_wikis_scatter.png")
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


def print_summary(africa: pd.DataFrame) -> None:
    rw = africa[africa["Language"] == HIGHLIGHT].iloc[0]
    rank_articles = int((africa["Articles"] > rw["Articles"]).sum()) + 1
    rank_ratio    = int((africa["articles_per_M_speakers"] > rw["articles_per_M_speakers"]).sum()) + 1
    total = len(africa)

    print(f"\n{'='*60}")
    print(f"Kinyarwanda Wikipedia vs {total} African-language Wikipedias")
    print(f"{'='*60}")
    print(f"  Articles          : {int(rw['Articles']):,}  (rank {rank_articles}/{total})")
    print(f"  Speakers (M)      : {rw['Total Speakers (M)']:.1f}")
    print(f"  Art/M speakers    : {rw['articles_per_M_speakers']:,.0f}  (rank {rank_ratio}/{total})")
    print()

    top5_art = africa.nlargest(5, "Articles")[["Language", "Articles"]]
    print("Top 5 by article count:")
    print(top5_art.to_string(index=False))
    print()

    top5_ratio = africa.nlargest(5, "articles_per_M_speakers")[["Language", "articles_per_M_speakers"]]
    print("Top 5 by articles per million speakers:")
    print(top5_ratio.to_string(index=False))
    print(f"{'='*60}\n")


def main() -> None:
    print(f"Loading {DATA_FILE} …")
    africa = load_africa(DATA_FILE)
    print(f"  {len(africa)} African-language Wikipedias loaded\n")

    print_summary(africa)

    print("Generating charts …")
    plot_articles(africa)
    plot_articles_per_speaker(africa)
    plot_scatter(africa)

    print("\nDone.")


if __name__ == "__main__":
    main()
