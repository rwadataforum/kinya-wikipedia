#!/usr/bin/env python3
"""
wikidata_extract_labels.py
==========================

Extracts English labels for a set of Q-IDs from a Wikidata JSON dump.

Typically used after ``wikidata_extract_p31.py`` to resolve the object Q-IDs
(the right-hand side of the P31 triplets) into human-readable English labels.

The dump is streamed directly from the bz2-compressed file, so no
decompression to disk is required.

Usage
-----
  # Default: reads output/p31_instances.tsv, outputs to output/p31_labels.tsv
  python wikidata_extract_labels.py

  # Custom paths
  python wikidata_extract_labels.py \\
      --input  output/p31_instances.tsv \\
      --dump   /path/to/wikidata-20231218-all.json.bz2 \\
      --out    output/p31_labels.tsv

  # Extract labels for the original subject Q-IDs instead
  python wikidata_extract_labels.py \\
      --input  wikipedia/output/rw_article_wikidata_ids.tsv \\
      --column wikidata_id \\
      --dump   /path/to/wikidata-20231218-all.json.bz2 \\
      --out    output/subject_labels.tsv

Input format
------------
  By default the script reads the ``object_qid`` column from a P31 triplet
  TSV file (produced by ``wikidata_extract_p31.py``).  Use ``--column`` to
  read from a different column name (e.g. ``wikidata_id``).

Output format
-------------
  Tab-separated file with two columns:

  ======  ============================
  qid     en_label
  ======  ============================
  Q5      human
  Q6256   country
  Q3624078 sovereign state
  ======  ============================

Resume support
--------------
  If the output file already exists, Q-IDs already present are skipped.
  Re-run the exact same command to continue.

Performance notes
-----------------
  Streaming through the full dump takes roughly 15-30 minutes on a modern SSD.
  The script stops early once every requested Q-ID has been found.
"""

import argparse
import bz2
import csv
import json
import time
import os


# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_INPUT = "output/p31_instances.tsv"
DEFAULT_COLUMN = "object_qid"
DEFAULT_DUMP = "wikidata-20231218-all.json.bz2"
DEFAULT_OUT = "output/p31_labels.tsv"


def load_qids(tsv_path: str, column: str) -> set[str]:
    """Load Q-IDs from the specified column of a TSV file."""
    qids = set()
    with open(tsv_path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            qid = row.get(column, "").strip()
            if qid.startswith("Q"):
                qids.add(qid)
    return qids


def load_done_qids(out_path: str) -> set[str]:
    """Return the set of Q-IDs already present in the output file."""
    done = set()
    try:
        with open(out_path, encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                done.add(row["qid"])
    except FileNotFoundError:
        pass
    return done


def extract_labels(dump_path: str, target_qids: set[str], out_path: str) -> None:
    """Stream through the Wikidata dump and extract English labels for target Q-IDs."""
    done_qids = load_done_qids(out_path)
    remaining = target_qids - done_qids

    if not remaining:
        print("All Q-IDs already processed. Nothing to do.")
        return

    print(f"Total Q-IDs to process : {len(target_qids)}")
    print(f"Already done           : {len(done_qids)}")
    print(f"Remaining              : {len(remaining)}")
    print()

    is_resume = len(done_qids) > 0

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    out_fh = open(out_path, "a", newline="", encoding="utf-8")
    writer = csv.writer(out_fh, delimiter="\t")
    if not is_resume:
        writer.writerow(["qid", "en_label"])

    total_found = 0
    processed = 0
    start_time = time.time()

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

                qid = entity.get("id", "")
                if qid not in remaining:
                    continue

                processed += 1
                labels = entity.get("labels", {})
                en_label = labels.get("en", {}).get("value", "")

                if en_label:
                    writer.writerow([qid, en_label])
                    total_found += 1

                remaining.discard(qid)
                out_fh.flush()

                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                print(
                    f"\r  Found {total_found} labels | "
                    f"Processed {processed}/{len(target_qids)} Q-IDs | "
                    f"{len(remaining)} remaining | "
                    f"{rate:.1f} Q-IDs/sec",
                    end="",
                    flush=True,
                )

                if not remaining:
                    break

    finally:
        out_fh.close()

    print()
    print(f"Done. {total_found} labels written to {out_path}")
    print(f"Elapsed time: {time.time() - start_time:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract English labels for Q-IDs from a Wikidata JSON dump."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Input TSV file containing Q-IDs",
    )
    parser.add_argument(
        "--column",
        default=DEFAULT_COLUMN,
        help="Column name in the input TSV that holds the Q-IDs (default: object_qid)",
    )
    parser.add_argument(
        "--dump",
        default=DEFAULT_DUMP,
        help="Path to the Wikidata JSON dump (.json.bz2)",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_OUT,
        help="Output TSV file path",
    )
    args = parser.parse_args()

    print(f"Input  : {args.input} (column: {args.column})")
    print(f"Dump   : {args.dump}")
    print(f"Output : {args.out}")
    print()

    qids = load_qids(args.input, args.column)
    print(f"Loaded {len(qids)} unique Q-IDs from {args.input}")
    print()

    extract_labels(args.dump, qids, args.out)


if __name__ == "__main__":
    main()
