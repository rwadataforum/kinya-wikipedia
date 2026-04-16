#!/usr/bin/env python3
"""
wikidata_query.py
=================

Query the Wikidata triplet table (Parquet or SQLite) produced by
``wikidata_to_triplets.py``.

Provides a simple CLI for common queries and a programmatic API for scripts.

Usage
-----
  # Get all P31 (instance of) triplets
  python wikidata_query.py --source output/wikidata_triplets/triplets.parquet \
      --property P31 \
      --out output/all_p31.tsv

  # Get all properties for a specific item
  python wikidata_query.py --source output/wikidata_triplets/triplets.parquet \
      --subject Q42 \
      --out output/q42_properties.tsv

  # Get all items that are instances of a specific class
  python wikidata_query.py --source output/wikidata_triplets/triplets.parquet \
      --object Q5 \
      --out output/all_humans.tsv

  # Multiple subjects or objects
  python wikidata_query.py --source output/wikidata_triplets/triplets.parquet \
      --subject Q42,Q183,Q15 \
      --property P31 \
      --out output/selected_p31.tsv

  # Query using SQLite instead of Parquet
  python wikidata_query.py --source output/wikidata_triplets/triplets.db \
      --property P31 \
      --out output/all_p31.tsv

  # Show statistics about the triplet table
  python wikidata_query.py --source output/wikidata_triplets/triplets.parquet \
      --stats

  # Count distinct subjects per property
  python wikidata_query.py --source output/wikidata_triplets/triplets.parquet \
      --property-counts

Query modes
-----------
  --subject QID[,QID...]   Filter by subject Q-ID(s)
  --property PID[,PID...]  Filter by property ID(s)
  --object QID[,QID...]    Filter by object Q-ID(s)
  --object-type TYPE       Filter by object type (item, string, time, etc.)
  --stats                  Show table statistics
  --property-counts        Show subject count per property

Output formats
--------------
  --out file.tsv           TSV output (default)
  --out file.csv           CSV output
  --out file.parquet       Parquet output
  --out file.jsonl         JSON Lines output
"""

import argparse
import json
import os
import sys
import time


# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_SOURCE = "output/wikidata_triplets/triplets.parquet"
DEFAULT_OUT = "output/query_results.tsv"


def _parse_list(value: str | None) -> list[str] | None:
    """Parse comma-separated string into list, or return None."""
    if not value:
        return None
    return [v.strip() for v in value.split(",") if v.strip()]


def _detect_source_type(source: str) -> str:
    """Detect whether source is Parquet or SQLite."""
    if source.endswith(".parquet"):
        return "parquet"
    if source.endswith(".db"):
        return "sqlite"
    # Try to detect by file existence
    if os.path.exists(source + ".parquet"):
        return "parquet"
    if os.path.exists(source + ".db"):
        return "sqlite"
    return "unknown"


def _load_parquet(source: str):
    """Load Parquet file into a pandas DataFrame."""
    import pandas as pd

    return pd.read_parquet(source)


def _query_parquet(
    source: str,
    subject: list[str] | None = None,
    property_id: list[str] | None = None,
    object_id: list[str] | None = None,
    object_type: str | None = None,
):
    """Query Parquet file with filters."""
    import pandas as pd

    print(f"Loading {source} ...")
    df = pd.read_parquet(source)
    print(f"  {len(df):,} triplets loaded")

    if subject:
        df = df[df["subject_qid"].isin(subject)]
    if property_id:
        df = df[df["property_id"].isin(property_id)]
    if object_id:
        df = df[df["object_qid"].isin(object_id)]
    if object_type:
        df = df[df["object_type"] == object_type]

    print(f"  {len(df):,} triplets after filtering")
    return df


def _query_sqlite(
    source: str,
    subject: list[str] | None = None,
    property_id: list[str] | None = None,
    object_id: list[str] | None = None,
    object_type: str | None = None,
):
    """Query SQLite database with filters."""
    import sqlite3
    import pandas as pd

    print(f"Connecting to {source} ...")
    conn = sqlite3.connect(source)

    conditions = []
    params = []

    if subject:
        placeholders = ",".join("?" for _ in subject)
        conditions.append(f"subject_qid IN ({placeholders})")
        params.extend(subject)
    if property_id:
        placeholders = ",".join("?" for _ in property_id)
        conditions.append(f"property_id IN ({placeholders})")
        params.extend(property_id)
    if object_id:
        placeholders = ",".join("?" for _ in object_id)
        conditions.append(f"object_qid IN ({placeholders})")
        params.extend(object_id)
    if object_type:
        conditions.append("object_type = ?")
        params.append(object_type)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    query = f"SELECT subject_qid, property_id, object_qid, object_type, object_value FROM triplets {where}"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    print(f"  {len(df):,} triplets returned")
    return df


def _save(df, out_path: str) -> None:
    """Save DataFrame to file based on extension."""
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    ext = os.path.splitext(out_path)[1].lower()

    if ext == ".tsv":
        df.to_csv(out_path, sep="\t", index=False)
    elif ext == ".csv":
        df.to_csv(out_path, sep=",", index=False)
    elif ext == ".parquet":
        df.to_parquet(out_path, index=False)
    elif ext == ".jsonl":
        df.to_json(out_path, orient="records", lines=True, force_ascii=False)
    else:
        # Default to TSV
        df.to_csv(out_path, sep="\t", index=False)

    print(f"  Saved to {out_path}")


def show_stats(source: str, source_type: str) -> None:
    """Show statistics about the triplet table."""
    if source_type == "parquet":
        df = _load_parquet(source)
    else:
        import sqlite3
        import pandas as pd

        conn = sqlite3.connect(source)
        df = pd.read_sql_query("SELECT * FROM triplets LIMIT 1", conn)
        conn.close()
        # Get counts from SQLite
        conn = sqlite3.connect(source)
        counts = pd.read_sql_query("SELECT COUNT(*) as cnt FROM triplets", conn)
        conn.close()
        print(f"Total triplets: {counts['cnt'].iloc[0]:,}")
        return

    print(f"Total triplets : {len(df):,}")
    print(f"Unique subjects: {df['subject_qid'].nunique():,}")
    print(f"Unique properties: {df['property_id'].nunique():,}")
    print(f"Unique objects : {df['object_qid'].nunique():,}")
    print()
    print("Object type distribution:")
    type_counts = df["object_type"].value_counts()
    for t, c in type_counts.items():
        print(f"  {t:20s} {c:>12,} ({c / len(df) * 100:.1f}%)")


def show_property_counts(source: str, source_type: str) -> None:
    """Show subject count per property."""
    if source_type == "parquet":
        import pandas as pd

        # Use pyarrow for efficient column read
        import pyarrow.parquet as pq

        table = pq.read_table(source, columns=["subject_qid", "property_id"])
        df = table.to_pandas()
    else:
        import sqlite3
        import pandas as pd

        conn = sqlite3.connect(source)
        df = pd.read_sql_query("SELECT subject_qid, property_id FROM triplets", conn)
        conn.close()

    counts = (
        df.groupby("property_id")["subject_qid"].nunique().sort_values(ascending=False)
    )

    print(f"{'Property':<12} {'Distinct Subjects':>20}")
    print("-" * 35)
    for prop, count in counts.items():
        print(f"{prop:<12} {count:>20,}")


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query Wikidata triplet table (Parquet or SQLite)."
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Path to Parquet file or SQLite database",
    )
    parser.add_argument(
        "--subject", default=None, help="Filter by subject Q-ID(s), comma-separated"
    )
    parser.add_argument(
        "--property",
        dest="property_id",
        default=None,
        help="Filter by property ID(s), comma-separated",
    )
    parser.add_argument(
        "--object",
        dest="object_id",
        default=None,
        help="Filter by object Q-ID(s), comma-separated",
    )
    parser.add_argument(
        "--object-type",
        default=None,
        help="Filter by object type (item, string, time, etc.)",
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output file path")
    parser.add_argument("--stats", action="store_true", help="Show table statistics")
    parser.add_argument(
        "--property-counts",
        action="store_true",
        help="Show distinct subject count per property",
    )
    args = parser.parse_args()

    source_type = _detect_source_type(args.source)
    if source_type == "unknown":
        print(f"Error: Cannot detect source type for '{args.source}'")
        print("Supported: .parquet or .db files")
        sys.exit(1)

    if args.stats:
        show_stats(args.source, source_type)
        return

    if args.property_counts:
        show_property_counts(args.source, source_type)
        return

    subject = _parse_list(args.subject)
    property_id = _parse_list(args.property_id)
    object_id = _parse_list(args.object_id)

    start_time = time.time()

    if source_type == "parquet":
        df = _query_parquet(
            args.source, subject, property_id, object_id, args.object_type
        )
    else:
        df = _query_sqlite(
            args.source, subject, property_id, object_id, args.object_type
        )

    if len(df) == 0:
        print("No results found.")
        return

    _save(df, args.out)

    elapsed = time.time() - start_time
    print(f"  Query time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()
