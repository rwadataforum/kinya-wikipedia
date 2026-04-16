#!/usr/bin/env python3
"""
wikidata_extract_p31.py
=======================

Extracts P31 (instance of) relationships from a Wikidata JSON dump for a list
of Q-IDs. Reads Q-IDs from a TSV file (produced by extract_article_wikidata_ids.py)
and outputs triplets in the form: subject_qid, P31, object_qid.

The Wikidata dump is streamed directly from the bz2-compressed file, so no
decompression to disk is required. Memory usage stays low regardless of dump size.

Usage
-----
  # Default: reads rw_article_wikidata_ids.tsv, outputs to output/p31_instances.tsv
  python wikidata_extract_p31.py

  # Custom paths
  python wikidata_extract_p31.py \\
      --input  wikipedia/output/rw_article_wikidata_ids.tsv \\
      --dump   /path/to/wikidata-20231218-all.json.bz2 \\
      --out    output/rw_p31_instances.tsv

Input format
------------
  TSV file with a ``wikidata_id`` column (e.g. Q42, Q183, …).
  Any row whose wikidata_id is empty or does not start with "Q" is skipped.

Output format
-------------
  Tab-separated file with three columns:

  =============  =====  ============
  subject_qid    P31    object_qid
  =============  =====  ============
  Q42            P31    Q5
  Q183           P31    Q6256
  Q183           P31    Q3624078
  =============  =====  ============

  Multiple rows are emitted when an entity has more than one P31 claim.

Resume support
--------------
  If the output file already exists, the script reads all subject_qid values
  that have already been written and skips them. Re-run the exact same command
  to continue from where it left off.

Performance notes
-----------------
  The Wikidata all.json.bz2 dump is ~90 GB compressed. Streaming through it
  takes roughly 15-30 minutes on a modern SSD. Only entities whose Q-ID is in
  the input list are parsed in detail, so the actual wall-clock time depends
  on how early in the dump your target Q-IDs appear.
"""

import argparse
import bz2
import csv
import json
import time
import os


# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_INPUT = "wikipedia/output/rw_article_wikidata_ids.tsv"
DEFAULT_DUMP = "wikidata-20231218-all.json.bz2"
DEFAULT_OUT = "output/p31_instances.tsv"


def load_qids(tsv_path: str) -> set[str]:
    """Load Q-IDs from the ``wikidata_id`` column of a TSV file."""
    qids = set()
    with open(tsv_path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            qid = row.get("wikidata_id", "").strip()
            if qid.startswith("Q"):
                qids.add(qid)
    return qids


def load_done_qids(out_path: str) -> set[str]:
    """Return the set of subject_qid values already present in the output file."""
    done = set()
    try:
        with open(out_path, encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                done.add(row["subject_qid"])
    except FileNotFoundError:
        pass
    return done


def extract_p31(dump_path: str, target_qids: set[str], out_path: str) -> None:
    """Stream through the Wikidata dump and extract P31 claims for target Q-IDs."""
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
        writer.writerow(["subject_qid", "P31", "object_qid"])

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
                claims = entity.get("claims", {})
                p31_claims = claims.get("P31", [])

                for claim in p31_claims:
                    mainsnak = claim.get("mainsnak", {})
                    if mainsnak.get("snaktype") != "value":
                        continue
                    datavalue = mainsnak.get("datavalue", {})
                    obj_qid = datavalue.get("value", {}).get("id", "")
                    if obj_qid:
                        writer.writerow([qid, "P31", obj_qid])
                        total_found += 1

                remaining.discard(qid)
                out_fh.flush()

                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                print(
                    f"\r  Found {total_found} P31 relations | "
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
    print(f"Done. {total_found} P31 triplets written to {out_path}")
    print(f"Elapsed time: {time.time() - start_time:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract P31 (instance of) relationships from a Wikidata JSON dump."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Input TSV file with a 'wikidata_id' column",
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

    print(f"Input : {args.input}")
    print(f"Dump  : {args.dump}")
    print(f"Output: {args.out}")
    print()

    qids = load_qids(args.input)
    print(f"Loaded {len(qids)} Q-IDs from {args.input}")
    print()

    extract_p31(args.dump, qids, args.out)


if __name__ == "__main__":
    main()
