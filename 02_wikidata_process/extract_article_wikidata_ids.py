#!/usr/bin/env python3
"""
extract_article_wikidata_ids.py

Reads article IDs and titles from an existing JSONL file, then queries the
MediaWiki API in batches to retrieve the Wikidata Q-ID (wikibase_item) for
each article.

Usage
-----
  # Test run – first 15 articles only
  python extract_article_wikidata_ids.py --test

  # Full run
  python extract_article_wikidata_ids.py

  # Custom paths / endpoint
  python extract_article_wikidata_ids.py \
      --input  wikipedia/output/rw_0.jsonl \
      --out    wikipedia/output/rw_article_wikidata_ids.tsv \
      --api    https://rw.wikipedia.org/w/api.php

# simple wikipedia 
      
 python extract_article_wikidata_ids.py \
      --input  wikipedia/output/simple_0.jsonl \
      --out    wikipedia/output/simple_article_wikidata_ids.tsv \
      --api    https://simple.wikipedia.org/w/api.php

      
Output
------
  Tab-separated file with three columns:
      article_id  <TAB>  title  <TAB>  wikidata_id

  wikidata_id is left empty when the API returns no Wikidata entry.

Resume support
--------------
  If the output file already exists, the script reads all article_ids already
  present and skips them.  Re-run the exact same command to continue from where
  it left off.
"""

import argparse
import json
import csv
import time

import requests

# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_INPUT = "wikipedia/output/rw_0.jsonl"
DEFAULT_OUT   = "wikipedia/output/rw_article_wikidata_ids.tsv"
DEFAULT_API   = "https://rw.wikipedia.org/w/api.php"

BATCH_SIZE   = 50    # MediaWiki API limit for anonymous requests
SLEEP_SECS   = 1.0  # pause between batches (be polite to the API)
TEST_LIMIT   = 15   # number of articles processed in --test mode
MAX_RETRIES  = 5    # max attempts per batch before giving up
BACKOFF_BASE = 2    # sleep = BACKOFF_BASE ** attempt  (2, 4, 8, 16, 32 s)


# ── step 1 : load articles from JSONL ────────────────────────────────────────

def load_articles(jsonl_path: str, limit: int | None = None) -> list[tuple[str, str]]:
    """
    Read (article_id, title) pairs from a JSONL file.
    Each line must have at minimum the keys "id" and "title".
    """
    articles = []
    with open(jsonl_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            article_id = str(record["id"])
            title      = record["title"]
            articles.append((article_id, title))
            if limit and len(articles) >= limit:
                break
    return articles


# ── resume support : find already-processed IDs ──────────────────────────────

def load_done_ids(out_path: str) -> set[str]:
    """
    Read the output TSV (if it exists) and return the set of article_ids
    that have already been written.  Used to skip work on resume.
    """
    done = set()
    try:
        with open(out_path, encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                done.add(row["article_id"])
    except FileNotFoundError:
        pass
    return done


# ── step 2 : fetch Wikidata Q-IDs from the MediaWiki API ─────────────────────

def fetch_wikidata_ids(api_url: str, titles: list[str]) -> dict[str, str]:
    """
    Query the MediaWiki 'pageprops' API for the 'wikibase_item' property.
    Returns {title: Q-ID} for titles that have a linked Wikidata entry.

    Retries up to MAX_RETRIES times with exponential backoff.
    On 429, honours the Retry-After header if present.
    """
    params = {
        "action":        "query",
        "prop":          "pageprops",
        "ppprop":        "wikibase_item",
        "titles":        "|".join(titles),
        "format":        "json",
        "formatversion": "2",
    }
    headers = {"User-Agent": "extract-wikidata-ids/1.0"}

    for attempt in range(MAX_RETRIES):
        resp = requests.get(api_url, params=params, timeout=30, headers=headers)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", BACKOFF_BASE ** (attempt + 1)))
            print(f"\n  Rate-limited (429). Waiting {retry_after} s "
                  f"(attempt {attempt + 1}/{MAX_RETRIES}) …")
            time.sleep(retry_after)
            continue

        resp.raise_for_status()
        data = resp.json()

        result = {}
        for page in data.get("query", {}).get("pages", []):
            title = page.get("title", "")
            qid   = page.get("pageprops", {}).get("wikibase_item", "")
            if title and qid:
                result[title] = qid
        return result

    raise RuntimeError(f"Batch failed after {MAX_RETRIES} attempts (persistent 429).")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch Wikidata Q-IDs for Wikipedia articles listed in a JSONL file."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT,
                        help="Input JSONL file (must have 'id' and 'title' fields)")
    parser.add_argument("--out",   default=DEFAULT_OUT,
                        help="Output TSV file path")
    parser.add_argument("--api",   default=DEFAULT_API,
                        help="MediaWiki API endpoint URL")
    parser.add_argument("--test",  action="store_true",
                        help=f"Test mode: process first {TEST_LIMIT} articles only")
    args = parser.parse_args()

    limit = TEST_LIMIT if args.test else None

    print(f"Input : {args.input}")
    print(f"API   : {args.api}")
    print(f"Output: {args.out}")
    print(f"Mode  : {'TEST (first %d articles)' % TEST_LIMIT if args.test else 'FULL'}")
    print()

    # --- load articles ---
    print("Step 1 – loading articles from JSONL …")
    articles = load_articles(args.input, limit=limit)
    print(f"  Loaded {len(articles)} articles.")

    # --- check for already-processed IDs (resume support) ---
    done_ids = load_done_ids(args.out)
    remaining = [(aid, title) for aid, title in articles if aid not in done_ids]
    is_resume = len(done_ids) > 0

    if is_resume:
        print(f"  Resuming: {len(done_ids)} already done, {len(remaining)} remaining.")
    print()

    if not remaining:
        print("Nothing left to do.")
        return

    # --- open output file (write header only on first run, append on resume) ---
    out_fh = open(args.out, "a", newline="", encoding="utf-8")
    writer = csv.writer(out_fh, delimiter="\t")
    if not is_resume:
        writer.writerow(["article_id", "title", "wikidata_id"])

    # --- query API in batches, writing each batch immediately ---
    print("Step 2 – fetching Wikidata Q-IDs from API …")
    total_with_qid = 0

    try:
        for batch_start in range(0, len(remaining), BATCH_SIZE):
            batch        = remaining[batch_start : batch_start + BATCH_SIZE]
            batch_titles = [title for _, title in batch]

            qid_map = fetch_wikidata_ids(args.api, batch_titles)

            for article_id, title in batch:
                qid = qid_map.get(title, "")
                writer.writerow([article_id, title, qid])
                if qid:
                    total_with_qid += 1

            out_fh.flush()   # make sure the batch is on disk before moving on

            done_so_far = len(done_ids) + min(batch_start + BATCH_SIZE, len(remaining))
            print(f"  {done_so_far}/{len(articles)} processed  "
                  f"({total_with_qid} new Q-IDs this run)",
                  end="\r", flush=True)

            time.sleep(SLEEP_SECS)

    finally:
        out_fh.close()

    print(f"\n  Done. {total_with_qid} new Q-IDs written to {args.out}")


if __name__ == "__main__":
    main()
