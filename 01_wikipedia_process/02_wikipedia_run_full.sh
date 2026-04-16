#!/bin/bash
# Everything runs inside the container. Output persists on your host.

###  to download and parse the Kinyarwanda Wikipedia.
# bash 02_wikipedia_run_full.sh --language rw
### other examples 
#bash 02_wikipedia_run_full.sh --language en        # English Wikipedia
#bash 02_wikipedia_run_full.sh --language fr        # French Wikipedia
#bash 02_wikipedia_run_full.sh --language simple    # Simple Wikipedia
#bash 02_wikipedia_run_full.sh --language rw --url-limit 1 --record-limit 500  # Test with limits




PROJECT_DIR="$(pwd)"

docker run \
  --gpus all \
  -it \
  --rm \
  --name nemo-curator-cli \
  -v "${PROJECT_DIR}/wikipedia/data:/workspace/wikipedia/data" \
  -v "${PROJECT_DIR}/wikipedia/output:/workspace/wikipedia/output" \
  -v "${PROJECT_DIR}/download_simplewiki.py:/workspace/download_simplewiki.py" \
  nvcr.io/nvidia/nemo-curator:26.02 \
  bash -c "source /opt/venv/env.sh && python /workspace/download_simplewiki.py $*"
