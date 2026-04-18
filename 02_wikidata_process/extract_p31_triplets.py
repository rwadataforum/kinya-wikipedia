#!/usr/bin/env python3
"""
extract_p31_triplets.py
=======================

Builds P31 (instance of) triplets from pre-extracted Wikidata entities.

Reads the JSONL file of extracted entities, collects subject labels in a first
pass, collects all unique object Q-IDs, fetches their labels from the Wikidata
API in batches, then writes the final triplet table.

Output columns: subject_id, property, object_id, subject_label, object_label

Usage
-----
  python 02_wikidata_process/extract_p31_triplets.py

  # Custom paths
  python 02_wikidata_process/extract_p31_triplets.py \\
      --entities output/wikidata/extracted-entities_wiki_rw.jsonl \\
      --out      output/wikidata/rw_p31_triplets.tsv
"""

import argparse
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path


LANG_PRIORITY = ["en", "fr", "rw", "sw"]
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
BATCH_SIZE = 50


def best_label(labels: dict) -> str:
    for lang in LANG_PRIORITY:
        if lang in labels:
            return labels[lang]["value"]
    if labels:
        return next(iter(labels.values()))["value"]
    return ""


def load_labels_and_objects(entities_path: Path) -> tuple[dict[str, str], set[str]]:
    """First pass: collect subject labels and all object Q-IDs."""
    labels: dict[str, str] = {}
    object_ids: set[str] = set()
    with entities_path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entity = json.loads(line)
            except json.JSONDecodeError:
                continue
            qid = entity.get("id", "")
            if not qid:
                continue
            labels[qid] = best_label(entity.get("labels", {}))
            for claim in entity.get("claims", {}).get("P31", []):
                try:
                    snak = claim["mainsnak"]
                    if snak.get("snaktype") == "value":
                        object_ids.add(snak["datavalue"]["value"]["id"])
                except (KeyError, TypeError):
                    continue
    return labels, object_ids


def fetch_labels_from_api(qids: set[str]) -> dict[str, str]:
    """Fetch English-priority labels for a set of Q-IDs via the Wikidata API."""
    fetched: dict[str, str] = {}
    qid_list = list(qids)
    total = len(qid_list)
    print(f"  Fetching labels for {total:,} object Q-IDs from Wikidata API ...")
    for i in range(0, total, BATCH_SIZE):
        batch = qid_list[i : i + BATCH_SIZE]
        params = urllib.parse.urlencode({
            "action": "wbgetentities",
            "ids": "|".join(batch),
            "props": "labels",
            "languages": "|".join(LANG_PRIORITY),
            "format": "json",
        })
        url = f"{WIKIDATA_API}?{params}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "kinya-wikipedia-analysis/1.0 (https://github.com; almugabo@googlemail.com)"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            for qid, entity in data.get("entities", {}).items():
                fetched[qid] = best_label(entity.get("labels", {}))
        except Exception as exc:
            print(f"  Warning: batch {i//BATCH_SIZE + 1} failed ({exc}), skipping")
        if i + BATCH_SIZE < total:
            time.sleep(0.1)
        if (i // BATCH_SIZE + 1) % 20 == 0:
            print(f"  ... {i + BATCH_SIZE:,} / {total:,}")
    return fetched


def extract_triplets(entities_path: Path, labels: dict[str, str], out_path: Path) -> int:
    count = 0
    with entities_path.open() as fh, out_path.open("w") as out:
        out.write("subject_id\tproperty\tobject_id\tsubject_label\tobject_label\n")
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entity = json.loads(line)
            except json.JSONDecodeError:
                continue
            subject_id = entity.get("id", "")
            if not subject_id:
                continue
            subject_label = labels.get(subject_id, "")
            for claim in entity.get("claims", {}).get("P31", []):
                try:
                    snak = claim["mainsnak"]
                    if snak.get("snaktype") != "value":
                        continue
                    object_id = snak["datavalue"]["value"]["id"]
                except (KeyError, TypeError):
                    continue
                object_label = labels.get(object_id, "")
                out.write(f"{subject_id}\tP31\t{object_id}\t{subject_label}\t{object_label}\n")
                count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description="Extract P31 triplets from Wikidata entities JSONL.")
    parser.add_argument("--entities", default="output/wikidata/extracted-entities_wiki_rw.jsonl")
    parser.add_argument("--out", default="output/wikidata/rw_p31_triplets.tsv")
    args = parser.parse_args()

    entities_path = Path(args.entities)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Loading labels and collecting object Q-IDs from {entities_path} ...")
    labels, object_ids = load_labels_and_objects(entities_path)
    print(f"  {len(labels):,} subject entities indexed")
    print(f"  {len(object_ids):,} unique object Q-IDs found")

    missing = object_ids - labels.keys()
    if missing:
        api_labels = fetch_labels_from_api(missing)
        labels.update(api_labels)
        print(f"  {len(api_labels):,} object labels fetched from API")

    print(f"Writing triplets to {out_path} ...")
    count = extract_triplets(entities_path, labels, out_path)
    print(f"  {count:,} triplets written")


if __name__ == "__main__":
    main()
