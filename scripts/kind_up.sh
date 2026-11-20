#!/usr/bin/env bash
# Create the four-node kind cluster, build and load the image, and start
# Kafka (3 brokers) and Redis. Idempotent: reuses an existing cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
K="kubectl --context kind-eip"
kind get clusters | grep -qx eip || kind create cluster --config deploy/k8s/kind-config.yaml
docker build -t eip:dev .
kind load docker-image eip:dev --name eip
$K apply -f deploy/k8s/kafka.yaml -f deploy/k8s/redis.yaml
$K rollout status statefulset/kafka --timeout=300s
$K rollout status deploy/redis --timeout=120s
$K get pods -o wide
