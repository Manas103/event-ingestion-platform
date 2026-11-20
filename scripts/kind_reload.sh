#!/usr/bin/env bash
# Rebuild the image and load it into the running kind cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -q -t eip:dev .
kind load docker-image eip:dev --name eip
