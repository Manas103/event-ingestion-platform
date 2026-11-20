#!/usr/bin/env bash
# One failover experiment on the kind cluster (scripts/kind_up.sh first).
#
#   scripts/kind_failover.sh <scenario>
#
# scenarios:
#   baseline         no fault
#   gateway-crash    SIGKILL one gateway process (it restarts with its spool volume)
#   gateway-rolling  rolling restart of every gateway (graceful: cordon, then stop)
#   broker-crash     SIGKILL one Kafka broker (topic RF 3, min.insync 2, acks=all)
#   applier-crash    SIGKILL one consumer-group member (its partitions rebalance)
#
# Env: DEVICES (1000) READINGS (600) RATE (10/s per device) FAULT_AT (20 s).
# Writes results/<scenario>/{devicesim.log,verify.log,fault.txt,summary.txt}.
set -euo pipefail
cd "$(dirname "$0")/.."
SCEN=$1
DEVICES=${DEVICES:-1000}; READINGS=${READINGS:-600}; RATE=${RATE:-10}; FAULT_AT=${FAULT_AT:-20}
RUN=$(echo "$SCEN" | tr -cd 'a-z0-9-')-$(date +%s)
TOPIC=readings-$RUN; GROUP=applier-$RUN
OUT=results/$SCEN; rm -rf "$OUT"; mkdir -p "$OUT"
K="kubectl --context kind-eip"
BROKERS=kafka-0.kafka-headless.default.svc.cluster.local:9092,kafka-1.kafka-headless.default.svc.cluster.local:9092,kafka-2.kafka-headless.default.svc.cluster.local:9092
CORPUS="\"-devices=$DEVICES\", \"-readings=$READINGS\", \"-dup-fraction=0.05\", \"-seed=20261120\""

# SIGKILL the main process of one pod, found by its HOSTNAME (= pod name) on its kind node
kill9_pod() {
  local pod=$1 pattern=$2 node
  node=$($K get pod "$pod" -o jsonpath='{.spec.nodeName}')
  docker exec "$node" sh -c "for p in \$(pgrep -f '$pattern'); do
      if tr '\0' '\n' < /proc/\$p/environ 2>/dev/null | grep -qx 'HOSTNAME=$pod'; then kill -9 \$p && echo \"SIGKILL pid \$p ($pod on $node)\"; fi; done"
}

wait_job() { # name timeout_s: until the job succeeded or failed
  local i
  for ((i = 0; i < $2; i += 2)); do
    [ "$($K get job "$1" -o jsonpath='{.status.succeeded}')" = 1 ] && return 0
    [ "$($K get job "$1" -o jsonpath='{.status.failed}')" = 1 ] && return 1
    sleep 2
  done
  return 1
}

run_job() { # name cmd args
  sed -e "s|__NAME__|$1|" -e "s|__CMD__|$2|" -e "s|__ARGS__|$3|" deploy/k8s/job.yaml | $K apply -f - >/dev/null
}

echo "== reset platform for run $RUN"
$K delete statefulset gateway --ignore-not-found --wait=true >/dev/null
$K delete deploy applier --ignore-not-found --wait=true >/dev/null
$K delete jobs --all --ignore-not-found >/dev/null
$K delete pvc spool-gateway-0 spool-gateway-1 spool-gateway-2 --ignore-not-found >/dev/null
$K exec deploy/redis -- redis-cli FLUSHALL >/dev/null
sed -e "s|__TOPIC__|$TOPIC|g" -e "s|__GROUP__|$GROUP|g" deploy/k8s/platform.yaml | $K apply -f - >/dev/null
$K rollout status statefulset/gateway --timeout=240s >/dev/null
$K rollout status deploy/applier --timeout=120s >/dev/null
sleep 8 # appliers join the group and get partitions

echo "== devicesim: $DEVICES devices x $READINGS readings at $RATE/s"
run_job devicesim devicesim "\"-url=ws://gateway:8080/ingest?ack=1\", $CORPUS, \"-rate=$RATE\", \"-out=stdout\", \"-timeout=15m\""
$K wait --for=condition=Ready pod -l job-name=devicesim --timeout=120s >/dev/null
sleep "$FAULT_AT"

FAULT_MS=$(date +%s%3N)
case "$SCEN" in
  baseline)        echo "no fault" ;;
  gateway-crash)   kill9_pod gateway-1 /usr/local/bin/gateway ;;
  gateway-rolling) $K rollout restart statefulset/gateway ;;
  broker-crash)    kill9_pod kafka-1 kafka.Kafka ;;
  applier-crash)   kill9_pod "$($K get pod -l app=applier -o jsonpath='{.items[0].metadata.name}')" /usr/local/bin/applier ;;
  *) echo "unknown scenario $SCEN"; exit 2 ;;
esac | tee "$OUT/fault.txt"
echo "FAULT_MS=$FAULT_MS" >> "$OUT/fault.txt"

wait_job devicesim 900 || echo "devicesim job did not succeed"
$K logs job/devicesim > "$OUT/devicesim.log"
GW="gateway-0.gateway-headless:9090,gateway-1.gateway-headless:9090,gateway-2.gateway-headless:9090"
run_job eipctl eipctl "\"-targets=$GW\", \"stats\""
wait_job eipctl 60 || true
$K logs job/eipctl > "$OUT/gateway_stats.txt" 2>&1 || true
[ "$SCEN" = gateway-rolling ] && $K rollout status statefulset/gateway --timeout=240s >/dev/null

echo "== verify"
run_job verify verify "$CORPUS, \"-redis=redis:6379\", \"-brokers=$BROKERS\", \"-topic=$TOPIC\", \"-out=stdout\""
wait_job verify 600 || echo "verify job did not succeed"
$K logs job/verify > "$OUT/verify.log"
for p in $($K get pod -l app=gateway -o name); do
  echo "$p restarts=$($K get "$p" -o jsonpath='{.status.containerStatuses[0].restartCount}')"
done > "$OUT/pods.txt"
$K get pod -l 'app in (kafka,applier)' -o custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount,NODE:.spec.nodeName --no-headers >> "$OUT/pods.txt"
python3 scripts/analyze_failover.py "$OUT" | tee "$OUT/summary.txt"
