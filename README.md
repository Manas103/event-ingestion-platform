# Event Ingestion Platform

[![CI](https://github.com/Manas103/event-ingestion-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Manas103/event-ingestion-platform/actions/workflows/ci.yml)

A Go event ingestion pipeline that loses nothing when parts of it die:
devices stream protobuf readings over WebSockets to replicated gateways,
the gateways deduplicate and produce to a 3-broker Kafka cluster (or spool
to disk), and a partitioned consumer group applies every reading to Redis
idempotently. Every claim below comes from a fault actually injected into a
running cluster and a verifier that diffs the final state against an
independent reference.

- **Acked at-least-once delivery.** A reading is acked only after it is
  durable (acks=all with replication factor 3 and min.insync.replicas 2, or
  spooled to the replica's own volume); devices resend anything unacked
  after a reconnect.
- **Idempotent, partitioned consumers.** A Kafka consumer group applies
  readings with HSETNX and commits offsets after applying, so redeliveries
  and duplicates never change the stored state.
- **Operable.** A gRPC admin API on every gateway (stats, drain the spool,
  cordon) plus `grpc.health.v1` for Kubernetes probes; graceful SIGTERM that
  moves devices to other replicas.
- **Tested by breaking it.** Scripted failure experiments on a 4-node kind
  cluster (gateway SIGKILL, rolling restart, Kafka broker SIGKILL, consumer
  SIGKILL), a CI job that repeats the gateway crash on every push, and a
  Terraform deployment that runs the same check on AWS and tears itself down.

## Results

1,000 simulated devices, 600 readings each (5% of frames duplicated, every
device's order shuffled), 10,000 readings/s for about 63 s; the fault is
injected 20 s in. One run per scenario; raw outputs in `docs/platform/`
(kind) and `docs/aws/`.

| scenario (kind, 3 gateways, 3 brokers, 3 appliers) | lost | final state = reference | recovery |
|---|---|---|---|
| no fault | 0 of 600,000 | yes | end-to-end p50 20 ms, p99 53 ms |
| SIGKILL a gateway | 0 | yes | 324 devices reconnected, p50 18 ms, p99 841 ms |
| rolling restart of all gateways | 0 | yes | 1,771 reconnects, p99 3.9 s |
| SIGKILL a Kafka broker (the controller) | 0 | yes | acks back to 90% in 7.1 s, consumers resumed after 12.9 s |
| SIGKILL a consumer | 0 | yes | consumers resumed after 4.9 s |
| AWS EC2 (2 vCPUs, Docker), SIGKILL 1 of 2 gateways | 0 | yes | 500 devices reconnected, p50 326 ms, p99 394 ms |

What building it found (details and tests in [DESIGN.md](DESIGN.md)):

- The original edge was fire-and-forget, so a gateway crash silently lost
  whatever was in flight. Fixed with the ack protocol and a client resend window.
- The spool drain truncated its file before retrying, so a crash mid-drain
  lost spooled readings that had already been acked. Fixed with a drain that
  rewrites the file atomically after the retries.
- Consumers that joined before the topic existed were assigned nothing and
  never looked again; a full run landed 600,000 readings in Kafka and applied
  none. Fixed by waiting for the topic and watching for partition changes.
- The AWS run, where devices get both gateway URLs instead of one Service,
  showed every dropped device spending a failed dial on the dead gateway
  before moving on (500 of 500). Fixed, with a test. The fix did not shorten
  the 0.3 s gap there, which is most likely the 2-vCPU host running every
  component (not yet tested on a bigger one; DESIGN.md, On AWS).
- Broker-crash recovery is set by Kafka's own controller election, leader
  moves and the consumer-group rebalance (traced in the broker logs), not by
  client settings: tuning kafka-go and the broker session timeout did not
  measurably change it.

## Architecture

```
devices (devicesim)             gateway x3 (StatefulSet)           Kafka x3 (KRaft)          applier x3 (group)      Redis
WebSocket + protobuf --frame--> dedup, produce acks=all  -------->  12 partitions, RF 3  ---> HSETNX per reading ---> r:{device}
resend unacked       <--Ack---  or spool to own volume              min.insync 2              commit after apply
                                gRPC admin :9090, health probes
```

The same edge shape (dedup on a natural key, produce or spool, then a
consumer checked against a from-scratch recompute) is used by four source
modules: device telemetry, storefront events with segment membership in
Redis over a ClickHouse event store, delivery webhooks with ClickHouse
rollups and a React + TypeScript dashboard, and location pings with
per-cell supply rollups. Their designs and measurements are in DESIGN.md.

## Running it

Unit tests (no services needed):

```bash
go test -race ./...
```

Failure experiments on a local kind cluster (Docker, kind, kubectl):

```bash
scripts/kind_up.sh                       # 4-node cluster, image, Kafka x3, Redis
scripts/kind_failover.sh baseline        # or gateway-crash, gateway-rolling, broker-crash, applier-crash
cat results/gateway-crash/summary.txt
```

On AWS (Terraform, Docker, AWS credentials). Creates an ECR repository, an
S3 bucket and one EC2 instance reachable only through SSM, runs a baseline
and a gateway crash on it, copies the logs to `results/aws/`, then destroys
everything and checks that no tagged resource is left:

```bash
scripts/aws_run.sh
```

Operating a gateway:

```bash
go run ./cmd/gateway -addr :8080 -admin-addr :9090 -brokers localhost:9092 -topic readings -create-topic
go run ./cmd/eipctl -targets localhost:9090 stats
go run ./cmd/devicesim -url 'ws://localhost:8080/ingest?ack=1' -devices 100 -readings 100
```

## Layout

| path | what |
|---|---|
| `cmd/gateway`, `internal/wsserver`, `internal/ingest` | the edge: WebSocket server, acks, dedup, produce-or-spool, crash-safe spool |
| `internal/admin`, `proto/admin.proto`, `cmd/eipctl` | gRPC admin and health API and its client |
| `cmd/applier`, `internal/applier` | consumer group, idempotent apply to Redis |
| `cmd/devicesim`, `internal/simdevice` | acked device fleet with reconnect and resend |
| `cmd/verify` | end-to-end check against the reference, latency split, log duplicates |
| `deploy/k8s`, `scripts/kind_*` | kind cluster, Kafka KRaft StatefulSet, platform manifests, failure experiments |
| `deploy/aws`, `scripts/aws_run.sh` | Terraform for ECR, S3, IAM, EC2 and the AWS run |
| `internal/{eventingest,segment,deliverystats,locationping,supplyrollup}`, `web/` | the other source modules |
| `docs/` | every raw output the numbers above and in DESIGN.md come from |

## Scope, stated plainly

- Devices are simulated (real WebSocket clients over real TCP), and so are
  the source modules' inputs.
- The kind cluster runs on one laptop: the four "nodes" are containers on
  one machine, so the experiments cover process and pod failure and Kafka
  replication, not network partitions or disk loss between hosts.
- Each scenario was run once; small differences between runs (for example
  the broker-crash tuning comparison in DESIGN.md) are within noise.
- Delivery is at-least-once into Kafka; exactly-once holds for the stored
  state because the apply is idempotent, not because the log has no
  duplicates (it has 0.7% to 2.5% after a gateway failover).
