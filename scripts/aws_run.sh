#!/usr/bin/env bash
# Run the platform on AWS once and tear it down (deploy/aws/main.tf):
# terraform apply, push the image to ECR, wait for the instance to finish the
# baseline and gateway-crash experiments, copy the logs from S3 into
# results/aws/, terraform destroy, then confirm no tagged resource is left.
#
# Needs terraform, docker and AWS credentials. AWS=<cli> picks the CLI binary
# (from WSL on this machine: the Windows aws.exe, whose `aws login` session
# is re-exported before every step because those credentials are short-lived).
set -euo pipefail
cd "$(dirname "$0")/.."
AWS=${AWS:-aws}
RUN=${RUN_ID:-r$(date +%Y%m%d%H%M)}
REGION=${REGION:-us-east-1}
TF="terraform -chdir=deploy/aws"
OUT=results/aws
mkdir -p "$OUT"

creds() { eval "$("$AWS" configure export-credentials --format env | tr -d '\r')"; export AWS_REGION=$REGION; }
destroy() {
  creds
  $TF destroy -auto-approve -input=false -var run_id="$RUN" | tail -3
  # the tagging API keeps listing terminated instances and deleted volumes for
  # a while, so check the real state of everything this run could leave behind
  {
    echo "instances not terminated: $("$AWS" ec2 describe-instances --region "$REGION" \
      --filters Name=tag:project,Values=event-ingestion-platform \
      Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down \
      --query 'length(Reservations[].Instances[])' --output text | tr -d '\r')"
    echo "volumes: $("$AWS" ec2 describe-volumes --region "$REGION" \
      --filters Name=tag:project,Values=event-ingestion-platform --query 'length(Volumes)' --output text | tr -d '\r')"
    echo "ECR repositories: $("$AWS" ecr describe-repositories --region "$REGION" \
      --query "length(repositories[?repositoryName=='event-ingestion-platform'])" --output text | tr -d '\r')"
    echo "S3 result buckets: $("$AWS" s3api list-buckets \
      --query "length(Buckets[?starts_with(Name, 'eip-results-')])" --output text | tr -d '\r')"
  } | tee "$OUT/left_after_destroy.txt"
}
trap destroy EXIT

creds
$TF init -input=false >/dev/null
$TF apply -auto-approve -input=false -var run_id="$RUN" | tail -8
REPO=$($TF output -raw ecr_repository_url)
BUCKET=$($TF output -raw results_bucket)

creds
# a throwaway docker config: the default one may name a credential helper
# (Docker Desktop's, here) that cannot run from this shell
export DOCKER_CONFIG
DOCKER_CONFIG=$(mktemp -d)
"$AWS" ecr get-login-password --region "$REGION" | tr -d '\r' | docker login --username AWS --password-stdin "${REPO%%/*}"
docker build -q -t "$REPO:latest" .
docker push -q "$REPO:latest"

echo "waiting for the instance to finish (about 6 minutes)"
for i in $(seq 1 90); do
  creds
  if "$AWS" s3 ls "s3://$BUCKET/$RUN/DONE" --region "$REGION" >/dev/null 2>&1; then break; fi
  sleep 20
done
"$AWS" s3 cp "s3://$BUCKET/$RUN/" "$(wslpath -w "$OUT" 2>/dev/null || echo "$OUT")" --recursive --region "$REGION" >/dev/null
ls "$OUT"
for e in baseline gateway-crash; do
  python3 scripts/analyze_failover.py "$OUT" "$e-" | tee "$OUT/$e-summary.txt"
done
