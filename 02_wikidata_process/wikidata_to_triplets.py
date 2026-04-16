#!/usr/bin/env python3
"""
wikidata_to_triplets.py
=======================

Converts a Wikidata JSON dump (.json.bz2) into a flat triplet table stored as
both Parquet and SQLite.

Each claim in every entity becomes one row:

    subject_qid  property_id  object_qid  object_type  object_value
    Q42          P31          Q5          item         Q5
    Q42          P569         None        time         1840-04-02T00:00:00Z
    Q183         P1082        None        quantity     83019000
    Q183         P31          Q6256       item         Q6256

Parquet is the primary output (columnar, fast analytical queries, highly
compressed).  SQLite is optional and useful for interactive exploration.

Usage
-----
  # Full conversion (Parquet only)
  python wikidata_to_triplets.py \\
      --dump /path/to/wikidata-20231218-all.json.bz2 \\
      --out  /path/to/output/

  # Also build a SQLite database
  python wikidata_to_triplets.py \\
      --dump /path/to/wikidata-20231218-all.json.bz2 \\
      --out  /path/to/output/ \\
      --sqlite

  # Limit to first N entities (for testing)
  python wikidata_to_triplets.py --dump ... --out ... --limit 10000

Output files
------------
  output/
    triplets.parquet          -- columnar triplet table (snappy compressed)
    triplets_metadata.json    -- conversion metadata
    triplets.db               -- SQLite database (only if --sqlite)

Performance
-----------
  On a modern SSD, converting the full 81 GB dump takes ~30-60 minutes.
  The resulting Parquet file is typically 10-20 GB.

  Memory usage stays low (< 2 GB) because data is flushed to disk in batches.
"""

import argparse
import bz2
import json
import os
import time
import re
from datetime import datetime

# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_DUMP = "wikidata-20231218-all.json.bz2"
DEFAULT_OUT = "output/wikidata_triplets"

# Batch size for writing to disk
BATCH_SIZE = 500_000

# ── datatype patterns ────────────────────────────────────────────────────────

_TIME_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T")
_GLOBE_COORD_RE = re.compile(r"Q\d+")


def _extract_object(mainsnak: dict) -> tuple[str | None, str, str]:
    """
    Extract (object_qid, object_type, object_value) from a mainsnak.

    Returns
    -------
    object_qid : str or None
        The Q-ID if the object is a wikibase-entityid, else None.
    object_type : str
        One of: item, property, string, time, quantity, monolingualtext,
        globecoordinate, somevalue, novalue.
    object_value : str
        Human-readable value.
    """
    snaktype = mainsnak.get("snaktype")
    if snaktype == "somevalue":
        return None, "somevalue", "somevalue"
    if snaktype == "novalue":
        return None, "novalue", "novalue"
    if snaktype != "value":
        return None, "unknown", ""

    datavalue = mainsnak.get("datavalue")
    if datavalue is None:
        return None, "unknown", ""

    dtype = datavalue.get("type", "")
    value = datavalue.get("value", {})

    if dtype == "wikibase-entityid":
        obj_id = value.get("id", "")
        entity_type = value.get("entity-type", "")
        return obj_id, entity_type or "item", obj_id

    if dtype == "string":
        return None, "string", str(value)

    if dtype == "monolingualtext":
        lang = value.get("language", "")
        text = value.get("text", "")
        return None, "monolingualtext", f"{lang}:{text}" if lang else text

    if dtype == "time":
        # Wikidata stores time as +2020-01-01T00:00:00Z
        raw = value.get("time", "")
        calendar = value.get("calendarmodel", "")
        # Extract just the date part
        m = _TIME_RE.search(raw)
        if m:
            clean = raw.lstrip("+")
            return None, "time", clean
        return None, "time", raw

    if dtype == "quantity":
        amount = value.get("amount", "")
        unit = value.get("unit", "")
        # Strip leading + from amount
        amount = amount.lstrip("+")
        if unit and unit != "1":
            unit_id = unit.split("/")[-1]
            return None, "quantity", f"{amount} [{unit_id}]"
        return None, "quantity", amount

    if dtype == "globecoordinate":
        lat = value.get("latitude", "")
        lon = value.get("longitude", "")
        globe = value.get("globe", "")
        globe_id = globe.split("/")[-1] if globe else ""
        return None, "globecoordinate", f"{lat},{lon} [{globe_id}]"

    return None, dtype, str(value)


def _entity_to_triplets(entity: dict) -> list[tuple[str, str, str | None, str, str]]:
    """Convert all claims of an entity into triplet rows."""
    qid = entity.get("id", "")
    if not qid:
        return []

    rows = []
    claims = entity.get("claims", {})

    for prop_id, claim_list in claims.items():
        for claim in claim_list:
            mainsnak = claim.get("mainsnak", {})
            obj_qid, obj_type, obj_value = _extract_object(mainsnak)
            rows.append((qid, prop_id, obj_qid, obj_type, obj_value))

    return rows


# ── main conversion ──────────────────────────────────────────────────────────


def convert(dump_path: str, out_dir: str, use_sqlite: bool, limit: int | None) -> None:
    os.makedirs(out_dir, exist_ok=True)

    # Import here so the script fails fast if deps are missing
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_path = os.path.join(out_dir, "triplets.parquet")
    metadata_path = os.path.join(out_dir, "triplets_metadata.json")

    # Arrow schema
    schema = pa.schema(
        [
            ("subject_qid", pa.string()),
            ("property_id", pa.string()),
            ("object_qid", pa.string()),
            ("object_type", pa.string()),
            ("object_value", pa.string()),
        ]
    )

    # We'll write Parquet in batches using a ParquetWriter
    writer = pq.ParquetWriter(parquet_path, schema, compression="snappy")

    # Optional SQLite
    sqlite_conn = None
    if use_sqlite:
        import sqlite3

        db_path = os.path.join(out_dir, "triplets.db")
        sqlite_conn = sqlite3.connect(db_path)
        sqlite_conn.execute("PRAGMA journal_mode=WAL")
        sqlite_conn.execute("PRAGMA synchronous=NORMAL")
        sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS triplets (
                subject_qid  TEXT,
                property_id  TEXT,
                object_qid   TEXT,
                object_type  TEXT,
                object_value TEXT
            )
        """)
        sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_subject ON triplets(subject_qid)
        """)
        sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_property ON triplets(property_id)
        """)
        sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_object ON triplets(object_qid)
        """)

    total_entities = 0
    total_triplets = 0
    batch_rows = []
    start_time = time.time()
    last_report = time.time()

    print(f"Dump  : {dump_path}")
    print(f"Output: {out_dir}/")
    print(f"SQLite: {'yes' if use_sqlite else 'no'}")
    print(f"Limit : {limit if limit else 'none (full dump)'}")
    print()

    try:
        with bz2.open(dump_path, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line in ("[", "]"):
                    continue
                if line.endswith(","):
                    line = line[:-1]

                try:
                    entity = json.loads(line)
                except json.JSONDecodeError:
                    continue

                total_entities += 1
                rows = _entity_to_triplets(entity)
                total_triplets += len(rows)
                batch_rows.extend(rows)

                # Flush batch
                if len(batch_rows) >= BATCH_SIZE:
                    _flush_batch(batch_rows, writer, sqlite_conn)
                    batch_rows = []

                # Progress report every 10 seconds
                now = time.time()
                if now - last_report >= 10:
                    elapsed = now - start_time
                    rate = total_entities / elapsed if elapsed > 0 else 0
                    print(
                        f"\r  Entities: {total_entities:,} | "
                        f"Triplets: {total_triplets:,} | "
                        f"Rate: {rate:,.0f} entities/s | "
                        f"Elapsed: {elapsed / 60:.1f} min",
                        end="",
                        flush=True,
                    )
                    last_report = now

                if limit and total_entities >= limit:
                    break

    finally:
        # Flush remaining
        if batch_rows:
            _flush_batch(batch_rows, writer, sqlite_conn)
        writer.close()
        if sqlite_conn:
            sqlite_conn.commit()
            sqlite_conn.close()

    elapsed = time.time() - start_time

    # Write metadata
    meta = {
        "dump": dump_path,
        "converted_at": datetime.now().isoformat(),
        "total_entities": total_entities,
        "total_triplets": total_triplets,
        "parquet_path": parquet_path,
        "elapsed_seconds": round(elapsed, 1),
    }
    with open(metadata_path, "w") as f:
        json.dump(meta, f, indent=2)

    print()
    print(f"\nConversion complete:")
    print(f"  Entities : {total_entities:,}")
    print(f"  Triplets : {total_triplets:,}")
    print(f"  Parquet  : {parquet_path}")
    if use_sqlite:
        print(f"  SQLite   : {os.path.join(out_dir, 'triplets.db')}")
    print(f"  Time     : {elapsed / 60:.1f} min")
    print(f"  Metadata : {metadata_path}")


def _flush_batch(rows: list[tuple], writer, sqlite_conn) -> None:
    """Write a batch of rows to Parquet and optionally SQLite."""
    import pyarrow as pa

    if not rows:
        return

    subject_qids = [r[0] for r in rows]
    property_ids = [r[1] for r in rows]
    object_qids = [r[2] for r in rows]
    object_types = [r[3] for r in rows]
    object_values = [r[4] for r in rows]

    table = pa.table(
        {
            "subject_qid": pa.array(subject_qids, type=pa.string()),
            "property_id": pa.array(property_ids, type=pa.string()),
            "object_qid": pa.array(object_qids, type=pa.string()),
            "object_type": pa.array(object_types, type=pa.string()),
            "object_value": pa.array(object_values, type=pa.string()),
        }
    )
    writer.write_table(table)

    if sqlite_conn:
        sqlite_conn.executemany(
            "INSERT INTO triplets VALUES (?, ?, ?, ?, ?)",
            rows,
        )


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Wikidata JSON dump to Parquet/SQLite triplet table."
    )
    parser.add_argument(
        "--dump", default=DEFAULT_DUMP, help="Path to wikidata JSON dump (.json.bz2)"
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output directory")
    parser.add_argument(
        "--sqlite", action="store_true", help="Also create a SQLite database"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit to first N entities (for testing)",
    )
    args = parser.parse_args()

    convert(args.dump, args.out, args.sqlite, args.limit)


if __name__ == "__main__":
    main()
