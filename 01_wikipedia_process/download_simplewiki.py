"""
Download and extract Simple Wikipedia using NeMo Curator.

This script can run in two modes:
1. Standalone mode: Starts its own Ray cluster (run inside the container)
2. Connected mode: Connects to an existing Ray cluster (from marimo notebook on host)

Usage:
  # Standalone (inside container):
  python download_simplewiki.py

  # Connected to external Ray cluster (from marimo on host):
  python download_simplewiki.py --ray-address ray://localhost:10001
"""

import argparse
import os
from pathlib import Path

from nemo_curator.core.client import RayClient
from nemo_curator.pipeline.pipeline import Pipeline
from nemo_curator.stages.text.download.wikipedia.stage import (
    WikipediaDownloadExtractStage,
)
from nemo_curator.stages.text.io.writer.jsonl import JsonlWriter


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Simple Wikipedia with NeMo Curator"
    )
    parser.add_argument(
        "--ray-address",
        type=str,
        default=None,
        help="Ray cluster address (e.g., ray://localhost:10001). "
        "If None, starts a local Ray cluster.",
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default="/workspace/wikipedia/data",
        help="Directory to store downloaded .bz2 files",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/workspace/wikipedia/output",
        help="Directory to write JSONL output files",
    )
    parser.add_argument(
        "--language",
        type=str,
        default="simple",
        help="Wikipedia language code (default: simple for Simple Wikipedia)",
    )
    parser.add_argument(
        "--url-limit",
        type=int,
        default=None,
        help="Maximum number of dump files to download. None = download all (default).",
    )
    parser.add_argument(
        "--record-limit",
        type=int,
        default=None,
        help="Maximum number of articles per dump file. None = extract all (default).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Create output directories
    Path(args.download_dir).mkdir(parents=True, exist_ok=True)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    # Initialize Ray client
    print(f"Initializing Ray client (address={args.ray_address or 'local'})...")
    try:
        if args.ray_address:
            ray_client = RayClient(address=args.ray_address)
        else:
            ray_client = RayClient()
        ray_client.start()
        print("Ray cluster started successfully.")
    except Exception as e:
        raise RuntimeError(f"Error initializing Ray client: {e}") from e

    try:
        # Create download and extract stage
        print(f"Creating Wikipedia pipeline for '{args.language}'...")
        stage = WikipediaDownloadExtractStage(
            language=args.language,
            download_dir=args.download_dir,
            url_limit=args.url_limit,
            record_limit=args.record_limit,
            verbose=True,
        )

        # Build pipeline
        pipeline = Pipeline("wikipedia_simple")
        pipeline.add_stage(stage)

        # Run pipeline
        print("Running download and extract pipeline...")
        results = pipeline.run()

        print(f"Pipeline completed. Got {len(results)} result batches.")

        # Write results to JSONL
        print(f"Writing results to {args.output_dir}...")
        jsonl_writer = JsonlWriter(
            path=args.output_dir, write_kwargs={"force_ascii": False}
        )

        for idx, result in enumerate(results):
            output_path = os.path.join(args.output_dir, f"{args.language}_{idx}.jsonl")
            jsonl_writer.write_data(result, output_path)
            print(f"  Written: {output_path}")

        print(f"\nDone! JSONL files saved to: {args.output_dir}")
        print(f"Downloaded .bz2 files stored in: {args.download_dir}")

    finally:
        # Stop Ray cluster (only if we started it locally)
        if not args.ray_address:
            print("Stopping Ray cluster...")
            try:
                ray_client.stop()
            except Exception as e:
                print(f"Warning: Error stopping Ray client: {e}")


if __name__ == "__main__":
    main()
