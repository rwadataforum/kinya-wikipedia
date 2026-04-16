#!/bin/bash
# NeMo Curator Docker Setup
# Everything runs inside the container - no local installation needed.

PROJECT_DIR="$(pwd)"

# =============================================================================
# STEP 0: Install NVIDIA Container Toolkit (run only once)
# =============================================================================
# The error "no known GPU vendor found" means Docker can't see your GPU.
# You need the NVIDIA Container Toolkit. Run these commands ONCE:
#
#   # 1. Add NVIDIA package repositories
#   curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
#   curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
#     sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
#     sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
#
#   # 2. Install the toolkit
#   sudo apt-get update
#   sudo apt-get install -y nvidia-container-toolkit
#
#   # 3. Configure Docker to use the NVIDIA runtime
#   sudo nvidia-ctk runtime configure --runtime=docker
#
#   # 4. Restart Docker
#   sudo systemctl restart docker
#
# After that, verify with:  docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi
# =============================================================================

# 1. Pull the NeMo Curator container
docker pull nvcr.io/nvidia/nemo-curator:26.02

# 2. Create persistent directories
mkdir -p "${PROJECT_DIR}/wikipedia/data"
mkdir -p "${PROJECT_DIR}/wikipedia/output"

# 3. Run Jupyter Notebook inside the container
#    Access at http://localhost:8888
docker run \
  --gpus all \
  -it \
  --rm \
  --name nemo-curator \
  -p 8888:8888 \
  -p 8265:8265 \
  -v "${PROJECT_DIR}/wikipedia/data:/workspace/wikipedia/data" \
  -v "${PROJECT_DIR}/wikipedia/output:/workspace/wikipedia/output" \
  -v "${PROJECT_DIR}/nemo_wikipedia_notebook.ipynb:/workspace/nemo_wikipedia_notebook.ipynb" \
  -v "${PROJECT_DIR}/download_simplewiki.py:/workspace/download_simplewiki.py" \
  nvcr.io/nvidia/nemo-curator:26.02 \
  bash -c "source /opt/venv/env.sh && jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root /workspace"
