#!/usr/bin/env python3
"""
wikidata_extract_items.py
=========================

Fast extraction of specific Q-IDs from a Wikidata bz2 dump.

Uses ``bzcat`` piped through ``grep -F`` (fixed-string matching) for speed,
then validates matches with Python JSON parsing to avoid false positives
(e.g. Q1 matching inside Q12345).

This is much faster than pure-Python streaming when you need only a small
subset of entities from the dump.

Usage
-----
  # Extract Q-IDs listed in a file (one per line)
  python wikidata_extract_items.py \\
      --dump /path/to/wikidata-20231218-all.json.bz2 \\
      --ids Q42,Q183,Q15,Q55 \\
      --out output/extracted.jsonl

  # Read Q-IDs from a file
  python wikidata_extract_items.py \\
      --dump /path/to/wikidata-20231218-all.json.bz2 \\
      --ids-file my_qids.txt \\
      --out output/extracted.jsonl

  # Extract and output as JSON (pretty)
  python wikidata_extract_items.py \\
      --dump /path/to/wikidata-20231218-all.json.bz2 \\
      --ids Q42,Q183 \\
      --out output/extracted.jsonl \\
      --format json

  # Extract and output as compact JSONL (default)
  python wikidata_extract_items.py \\
      --dump /path/to/wikidata-20231218-all.json.bz2 \\
      --ids Q42,Q183 \\
      --out output/extracted.jsonl \\
      --format jsonl

How it works
------------
  1. Build a ``grep -F`` pattern that matches ``"id":"Q42"`` for each Q-ID.
     The quotes around the id prevent false positives (Q1 won't match Q12345).
  2. ``bzcat dump.json.bz2 | grep -F -f patterns.txt`` decompresses once and
     does a fast fixed-string search.
  3. Python parses each matched line as JSON and verifies the entity id is
     actually in the target set.
  4. Results are written to the output file.

Performance
-----------
  For a handful of Q-IDs from an 81 GB dump, this takes ~1-3 minutes
  (dominated by decompression speed). Pure Python streaming would take
  15-30 minutes for the same task.
"""

import argparse
import bz2
import json
import os
import subprocess
import sys
import tempfile
import time


# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_DUMP = "wikidata-20231218-all.json.bz2"
DEFAULT_OUT = "output/extracted.jsonl"
DEFAULT_FMT = "jsonl"


def _parse_ids(ids_str: str | None, ids_file: str | None) -> set[str]:
    """Parse Q-IDs from comma-separated string or file."""
    qids = set()
    if ids_str:
        for qid in ids_str.split(","):
            qid = qid.strip()
            if qid:
                qids.add(qid)
    if ids_file:
        with open(ids_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue
                qids.add(line)
    return qids


def _build_grep_patterns(qids: set[str]) -> str:
    """
    Build grep -F patterns that match "id":"Q42" exactly.
    The surrounding quotes prevent Q1 from matching Q12345.
    """
    patterns = []
    for qid in qids:
        patterns.append(f'"id":"{qid}"')
    return "\n".join(patterns)


def extract_with_grep(dump_path: str, qids: set[str], out_path: str, fmt: str) -> int:
    """
    Extract entities using bzcat | grep -F pipeline.

    Returns the number of entities found.
    """
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    # Write patterns to a temp file
    patterns = _build_grep_patterns(qids)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, encoding="utf-8"
    ) as pf:
        pf.write(patterns)
        pattern_file = pf.name

    try:
        # bzcat dump | grep -F -f patterns
        bzcat = subprocess.Popen(
            ["bzcat", dump_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        grep = subprocess.Popen(
            ["grep", "-F", "-f", pattern_file],
            stdin=bzcat.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Close bzcat's stdout so it can get SIGPIPE when grep exits
        bzcat.stdout.close()

        found = 0
        found_qids = set()

        with open(out_path, "w", encoding="utf-8") as out_fh:
            for line in grep.stdout:
                line = line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                if line.endswith(","):
                    line = line[:-1]

                try:
                    entity = json.loads(line)
                except json.JSONDecodeError:
                    continue

                entity_id = entity.get("id", "")
                if entity_id not in qids:
                    continue

                found += 1
                found_qids.add(entity_id)

                if fmt == "json":
                    out_fh.write(json.dumps(entity, indent=2, ensure_ascii=False))
                    out_fh.write("\n")
                else:
                    out_fh.write(json.dumps(entity, ensure_ascii=False))
                    out_fh.write("\n")

                if found % 100 == 0:
                    print(
                        f"\r  Found {found} entities ({len(found_qids)} unique Q-IDs)",
                        end="",
                        flush=True,
                    )

        print()

        # Check if bzcat or grep failed
        bzcat.wait()
        grep.wait()

    finally:
        os.unlink(pattern_file)

    return found


def extract_pure_python(dump_path: str, qids: set[str], out_path: str, fmt: str) -> int:
    """
    Fallback: pure Python streaming (no bzcat/grep dependency).
    Slower but works everywhere.
    """
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    remaining = set(qids)
    found = 0
    start_time = time.time()

    with open(out_path, "w", encoding="utf-8") as out_fh:
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

                entity_id = entity.get("id", "")
                if entity_id not in remaining:
                    continue

                found += 1
                remaining.discard(entity_id)

                if fmt == "json":
                    out_fh.write(json.dumps(entity, indent=2, ensure_ascii=False))
                    out_fh.write("\n")
                else:
                    out_fh.write(json.dumps(entity, ensure_ascii=False))
                    out_fh.write("\n")

                elapsed = time.time() - start_time
                rate = found / elapsed if elapsed > 0 else 0
                print(
                    f"\r  Found {found}/{len(qids)} entities | "
                    f"{len(remaining)} remaining | {rate:.1f}/s",
                    end="",
                    flush=True,
                )

                if not remaining:
                    break

    print()
    return found


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fast extraction of specific Q-IDs from a Wikidata dump."
    )
    parser.add_argument(
        "--dump", default=DEFAULT_DUMP, help="Path to wikidata JSON dump (.json.bz2)"
    )
    parser.add_argument(
        "--ids", default=None, help="Comma-separated Q-IDs (e.g. Q42,Q183,Q15)"
    )
    parser.add_argument(
        "--ids-file", default=None, help="File with Q-IDs (one per line)"
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output file path")
    parser.add_argument(
        "--format",
        default=DEFAULT_FMT,
        choices=["jsonl", "json"],
        help="Output format: jsonl (compact) or json (pretty)",
    )
    parser.add_argument(
        "--python-only",
        action="store_true",
        help="Use pure Python instead of bzcat|grep (slower)",
    )
    args = parser.parse_args()

    qids = _parse_ids(args.ids, args.ids_file)
    if not qids:
        print("Error: No Q-IDs provided. Use --ids or --ids-file.")
        sys.exit(1)

    print(f"Dump   : {args.dump}")
    print(f"Q-IDs  : {len(qids)} entities requested")
    print(f"Output : {args.out}")
    print(f"Format : {args.format}")
    print()

    start_time = time.time()

    if args.python_only:
        print("Using pure Python streaming (slower)...")
        found = extract_pure_python(args.dump, qids, args.out, args.format)
    else:
        print("Using bzcat | grep -F (fast)...")
        found = extract_with_grep(args.dump, qids, args.out, args.format)

    elapsed = time.time() - start_time
    print(f"\nDone. Found {found} entities in {elapsed:.1f}s")
    print(f"Output: {args.out}")


if __name__ == "__main__":
    main()
