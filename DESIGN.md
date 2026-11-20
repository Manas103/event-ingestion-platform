# Design

## The platform

The gateway, the acked protocol, the consumer group and the cluster setup
that turn the source modules into one replicated pipeline.

```
devices (devicesim)              gateway StatefulSet (3)              Kafka (3 brokers, KRaft)      applier Deployment (3)    Redis
 WebSocket + protobuf --frame--> dedup, produce (acks=all)  ------->  12 partitions, RF 3,     ---> consumer group,      ---> r:{device}
 window of unacked    <--Ack---  or spool to its own volume           min.insync.replicas 2         HSETNX, commit after apply
 resend on reconnect             gRPC admin + grpc.health.v1
```

### Delivery guarantee

A reading is acked to the device only once it is durable: produced with
acks=all to a topic with replication factor 3 and min.insync.replicas 2, or
written to the replica's spool file, or recognized as a duplicate of one that
was. Each device keeps every unacked reading (a window of 64) and resends
them, in sequence order, after any reconnect. That gives at-least-once from
device to log. The edge dedup is in memory and per replica, so a device that
reconnects to a different replica can put a second copy in the log. The
appliers store with HSETNX on `(device_id, sequence)` and commit offsets only
after applying, so redeliveries and duplicates in the log are absorbed: the
stored state is exactly one value per reading.

What a spooled reading survives: the spool file lives on the replica's own
PersistentVolumeClaim and is drained by the same replica when it restarts.
A spooled record survives the process being killed (the write is in the
kernel page cache before the ack); with `-spool-fsync` it also survives the
node losing power, at one fsync per spooled record. It does not survive
losing the volume itself.

### Gateway

`cmd/gateway` serves devices on `:8080` (`/ingest`, `?ack=1` for the acked
protocol) and the admin API on `:9090`:

- `IngestAdmin.Stats`: per-replica counters (received, duplicates, produced,
  spooled, drained, acks, spool bytes, dedup entries, open connections).
- `IngestAdmin.DrainSpool`: retry the spool now.
- `IngestAdmin.Cordon`: readiness to NOT_SERVING, refuse new connections,
  close open ones so devices move to other replicas.
- `grpc.health.v1`, which the Kubernetes readiness probe calls directly
  (`grpc:` probe). Liveness is a TCP probe on the device port, so a cordoned
  replica is taken out of the Service without being restarted.

On SIGTERM a replica cordons itself, stops the HTTP server, runs one final
spool drain and exits; anything still spooled stays on its volume.
`cmd/eipctl` is the operator client (`stats`, `drain`, `cordon`, `health`).

### Bugs found and fixed while building it

1. **No acks, so a crash lost whatever was in flight.** The original edge was
   fire-and-forget: a device had no way to know which readings a gateway
   had kept when it died. The acked protocol and the client-side resend
   window fix this; `internal/wsserver/failover_test.go` cordons a replica
   mid-stream and checks the final state against the reference.
2. **The spool drain truncated before retrying.** `Spool.Drain` read every
   record, truncated the file, then retried; a gateway killed in between
   lost every spooled record, including ones it had already acked.
   `Spool.DrainWith` leaves the file in place while retrying, then
   atomically replaces it (temp file, fsync, rename) with the records still
   failing plus anything written meanwhile. Test:
   `TestDrainWithKeepsRecordsOnDiskUntilRetryFinishes`.
3. **A reading that could not be kept stayed in the dedup set.** When both
   the produce and the spool write failed, the reading was not durable but
   its `(device, sequence)` was already admitted, so the device's resend was
   dropped as a duplicate. `Dedup.Forget` rolls the entry back and the
   reading is not acked. Test:
   `TestUndurableReadingIsForgottenSoItsResendIsAdmitted`.
4. **Consumers that started before the topic existed never consumed.** The
   appliers and the gateways start together and the gateway creates the
   topic. kafka-go gives a group member that joins before the topic has
   partitions an empty assignment and, by default, never looks again: the
   first full run landed all 600,000 readings in Kafka and applied none. The
   applier now waits for the topic, and the reader watches for partition
   changes.
5. **A latency number that measured the wrong thing.** The first gateway
   crash run reported a p99.9 end-to-end latency of 48 s. Splitting the
   samples showed every slow one was a re-application of a duplicate copy,
   which carries its original first-send timestamp, so its "latency" was
   only how much later the duplicate came along; first applications had a
   p99.9 of 147 ms. `cmd/verify` now reports first applications,
   re-applications and the consumer side (Kafka timestamp to applied)
   separately.

### Failure experiments (kind, 4 nodes)

`scripts/kind_up.sh` creates a kind cluster (1 control plane, 3 workers),
builds the image and starts 3 Kafka 3.9.1 brokers in KRaft mode (one per
worker, each both broker and controller) and Redis.
`scripts/kind_failover.sh <scenario>` resets the platform on a fresh topic
and consumer group, runs `devicesim` in the cluster (1,000 devices x 600
readings, 5% of frames duplicated and every device's order shuffled, 10
readings/s per device, so 10,000 readings/s for about 63 s), injects the
fault 20 s in, then runs `cmd/verify`, `eipctl stats` and
`scripts/analyze_failover.py`. The faults are real: SIGKILL of the process
inside the pod (found by its HOSTNAME on the kind node), or a real rolling
restart. Raw outputs are in `docs/platform/<scenario>/`. Each scenario was
run once.

| scenario | fault | lost | duplicates in the log | lowest 1 s ack rate after the fault | recovery | first-application latency |
|---|---|---|---|---|---|---|
| baseline | none | 0 of 600,000 | 0 | 9,784/s of 10,000 | n/a | p50 20 ms, p99 53 ms, max 179 ms |
| gateway-crash | SIGKILL 1 of 3 gateways | 0 | 4,514 (0.747%) | 9,827/s | 324 reconnects: gap p50 18 ms, p99 841 ms, max 977 ms | p99 39 ms, max 139 ms |
| gateway-rolling | rolling restart of all 3 | 0 | 15,664 (2.544%) | 8,589/s | 1,771 reconnects: gap p50 52 ms, p99 3,860 ms, max 4,569 ms | p99 538 ms, max 1,203 ms |
| broker-crash | SIGKILL 1 of 3 brokers | 0 | 4,690 (0.776%) | 3,449/s | acks back to 90% in 7.1 s; consumers stalled 12.9 s | p99 12.5 s |
| applier-crash | SIGKILL 1 of 3 appliers | 0 | 0 (70 re-applied) | 10,000/s | consumers stalled 4.9 s | p99 4.8 s |

Every run ends with `RESULT: zero lost, zero extra, final state digest
identical to the reference: true`. The verifier rebuilds the same seeded
corpus, computes the expected state with the reference dedup, and diffs it
against every hash in Redis (missing, extra, wrong value, SHA-256 digest).

**Gateway crash.** The killed replica's devices redial through the Service,
land on the other two, and resend what was not acked (13 frames here). The
replica comes back with its own volume (restart count 1). The duplicates in
the log are the corpus's deliberate duplicate frames that reached a replica
with no memory of the original: per-replica dedup is an optimization, the
idempotent apply is the guarantee.

**Rolling restart.** Each replica cordons on SIGTERM, so devices are moved
rather than cut off, but a device can be moved up to three times as the
three replicas roll, hence more reconnects, a longer tail and more
duplicates than a single crash.

**Broker crash.** This is the slow one, and the time goes to Kafka's own
failover, not to the clients. In the 3 s session-timeout run
(`docs/platform/broker-crash/kafka-0_failover_excerpt.log`) the killed
broker was both the active KRaft controller and the group coordinator.
Peers saw the disconnect at +0.4 s; kafka-0 won the controller election at
+3.6 s, after one split vote; partition leadership and the group
coordinators moved to the survivors at +6.7 to +7.1 s; the consumer group
then needed a full rebalance on the new coordinator, about 6 s more.
Producers keep going meanwhile: writes to partitions the dead broker led
time out after the 1 s produce timeout and are spooled and acked (27,005
readings spooled across the three gateways, all drained), which is why the
ack rate dips to a third rather than to zero. The duplicates in the log
here come from the same place: a produce that timed out on the client can
still have been written by the time the leader moves, and the reading was
spooled and produced again; at-least-once accepts that copy and the
idempotent apply removes it. Two attempts to make recovery
faster did not measurably help:

| run (`docs/platform/...`) | broker.session.timeout.ms | kafka-go metadata TTL / group rejoin backoff | ack recovery | consumer stall |
|---|---|---|---|---|
| `client-defaults/broker-crash` | 9 s (default) | 6 s / 5 s (defaults) | 6.4 s | 13.8 s |
| `broker-crash-session9s` | 9 s (default) | 1 s / 0.5 s | 8.4 s | 13.8 s |
| `broker-crash` | 3 s | 1 s / 0.5 s | 7.1 s | 12.9 s |

With one run each, these differences are within run-to-run noise (the
controller election alone varies with split votes). Both settings are kept
because they bound the client's and the fencing share of a failover, not
because they were shown to shorten it. The remaining stall is the eager
consumer-group rebalance; kafka-go implements neither the incremental
consumer protocol (KIP-848) nor static membership, the usual ways to
shorten it.

**Applier crash.** The dead member's partitions stop until the group
coordinator notices the missed heartbeats (6 s session timeout) and
rebalances: 4.9 s with nothing applied. 70 readings from the dead member's
last uncommitted batch were applied again by the new owner, harmlessly. The
devices never notice: acks depend only on the log.

### Environment for these runs

kind v0.24.0 (Kubernetes v1.31.0) on Docker Engine 29.1.3 inside WSL2
Ubuntu 22.04 (12 vCPUs and 12 GB allotted) on one Windows 11 laptop. All four
kind nodes share that machine, so the nodes are containers, not separate
hosts: the experiments exercise process and pod failure, rescheduling and
Kafka replication for real, but not network partitions or disk loss between
machines. Kafka restart counts in `pods.txt` include restarts from WSL VM
restarts before these runs (WSL stops a distro's services when the last WSL
client exits), not from the experiments.

### On AWS

`scripts/aws_run.sh` applies `deploy/aws/main.tf` (an ECR repository, an
S3 bucket for results, an IAM role allowing only SSM, ECR pulls and writes
to that bucket, a security group with no inbound rules, and one
`m7i-flex.large` instance, 2 vCPUs and 8 GB, Amazon Linux 2023), builds and
pushes the image, and waits. The instance's user data
(`deploy/aws/run.sh.tftpl`) installs Docker, runs one Kafka 3.9.1 broker,
Redis, 2 gateways with their own spool volumes, 2 appliers and the same
1,000-device simulator against both gateway URLs, kills `gw-1` with SIGKILL
20 s into the second run, verifies, uploads everything to S3 and powers off.
The script then copies the results down, runs `terraform destroy`, and checks
that no instance, volume, ECR repository or results bucket is left
(`docs/aws/left_after_destroy.txt`). Every resource is free-tier eligible
and lives about ten minutes.

| run (`docs/aws/...`) | lost | duplicates in the log | failed redials | reconnect gap | first-application latency |
|---|---|---|---|---|---|
| `baseline-*` | 0 of 600,000 | 0 | 0 | n/a | p50 35 ms, p99 793 ms |
| `redial-same-url/gateway-crash-*` | 0 | 6,694 (1.103%) | 500 | n=500: p50 289 ms, p99 388 ms | p50 28 ms, p99 733 ms |
| `gateway-crash-*` | 0 | 6,639 (1.094%) | 0 | n=500: p50 326 ms, p99 394 ms | p50 28 ms, p99 802 ms |

Every run ends with the same `RESULT: ... true` line.

**A client bug the kind runs could not show.** On kind, devices dial one
Service URL and the Service picks a live pod. On AWS they are given both
gateway URLs, and the first run showed exactly 500 failed redials for the
500 devices on the killed gateway: after a dropped connection a device
redialed the same URL first, and only moved on after that dial failed. The
device now moves to the next URL when a connection drops
(`internal/simdevice/acked.go`), and `TestCordonMidStreamLosesNothing` fails
if any device redials the replica that dropped it (25 failed redials without
the fix, 0 with it). The rerun has 0 failed redials.

**What the fix did not change.** The reconnect gap stayed at about 0.3 s
(minimum 234 ms with no failed dial at all), against 18 ms on kind, so the
failed dial was not where the time went. The likely cause is the host: two
vCPUs run Kafka, Redis, four Go services, the restarting gateway and the
1,000-device simulator, and the baseline already shows it (first-application
p99 793 ms, against 53 ms on the 12-vCPU kind setup, while the consumer side,
Kafka timestamp to applied, stays at p99 89 ms). That explanation was not
tested on a larger instance.

## Source modules

Every source module has the same edge shape. A Go edge receives simulated
input, deduplicates it on a natural key, and produces it to Kafka; when Kafka
cannot take a record within its produce window, the edge either spools it to
a local disk file (modules 1 and 4) or reports a produce error (modules 2 and
3) instead of silently dropping it. A module-specific consumer then builds
state from what landed: a Kafka read-back digest, a Redis segment-membership
engine over a ClickHouse event store, ClickHouse per-minute rollups, or Redis
per-cell supply counts. Each module's result is checked against an
independent from-scratch recompute (a reference dedup, a full ClickHouse
recompute, or a `Recompute`-and-`Digest` oracle) that shares no evaluation
code with the path it checks.

Disclosures that apply to every module:

- **Inputs are simulated, stated as such.** Devices, storefront events,
  provider webhooks and location pings are generated by the `sim*` packages.
  Where a module uses WebSocket clients, they are real clients making real
  TCP connections to a real running edge, and Kafka, ClickHouse and Redis are
  real running processes, never mocked interfaces. No real device, customer,
  message provider or vehicle is involved.
- **Broker outages are simulated at the producer.** The "60-second broker
  outage" is a producer told to refuse writes for 60 seconds, not the Kafka
  process being stopped, so the measurement is deterministic and does not
  depend on how fast a real broker restarts on this machine.
- **Single-node backends.** One Kafka broker (no replication), one ClickHouse
  node, one Redis instance. None of the modules relies on a distributed
  guarantee; the dedup and spool logic exist so the edge does not need
  Kafka's own replication.
- **Native processes, not containers.** Kafka, ClickHouse and Redis ran as
  native WSL2 processes (scripts in `docs/_start_*.sh`). The delivery-stats
  Dockerfiles and compose file in `deploy/` were written but not built or
  run.
- **Raw output is verbatim.** Every number below is the literal output of a
  real run, kept unedited in `docs/`. Bugs found by the first measurement
  runs are recorded under each module's Findings, and every number was taken
  after the fix.

### 1. Device telemetry edge

Packages: `internal/ingest`, `internal/wsserver`, `internal/simdevice`,
`internal/kafkaclient` (`producer.go`, `consumer.go`). Commands:
`cmd/gateway`, `cmd/loadgen`, `cmd/outagebench`, `cmd/loadcurve`,
`cmd/soak`.

A Go WebSocket server takes one persistent connection per simulated device,
deduplicates on `(device_id, sequence)`, fans deduplicated readings out to
Kafka keyed by device id, and spills to a local disk file rather than
dropping a reading when Kafka cannot take it right now. The shape is a Go
server holding persistent protobuf-over-WebSocket connections from many
devices and dispatching what it receives to Kafka and other backends. Two
properties are tested directly: correctness under a lossy, duplicating,
reordering network (dedup has to be exactly right, not approximately right),
and survivability when the downstream broker goes away for a while (a slow
consumer or an outage should degrade into "queued on disk", not "silently
gone").

#### What it is and is not

- **5,000 simulated devices** as real WebSocket clients; the outage is
  simulated at the producer (see the shared disclosures).
- **A correctness module first, a throughput module second.** The replay and
  outage benchmarks measure whether the final state is exactly right and
  whether anything got lost, not how many messages per second the gateway
  sustains. The only rate measurement is the staged load curve, which runs
  against a fixed-capacity fake producer, not real Kafka.

#### Architecture

```
proto/
  telemetry.proto           # Reading: device_id, sequence, timestamp_ms, value
  telemetry.pb.go           # generated
internal/
  ingest/
    dedup.go                # per-device set of sequences already forwarded (bounded, see Findings)
    spool.go                # append-only length-prefixed disk overflow queue
    pipeline.go             # dedup -> produce; on failure -> spool; drainer retries
    reference.go            # independent dedup + order-independent digest, for diffing
    pipeline_test.go        # unit tests + the differential fuzz test
    dedup_cap_test.go       # bounded-cache regression tests
  kafkaclient/
    producer.go             # segmentio/kafka-go writer, keyed by device id
    consumer.go             # reads every message on a topic back out, for verification
  wsserver/
    server.go               # one WebSocket connection per device, binary protobuf frames
  simdevice/
    corpus.go               # builds and drives the duplicated/reordered device corpus
cmd/
  gateway/main.go           # standalone deployable service
  loadgen/main.go           # the 5,000-device replay-correctness benchmark
  outagebench/main.go       # the 60-second broker-outage benchmark
  loadcurve/main.go         # staged load curve against a fixed-capacity fake producer
  soak/main.go              # six-hour-volume soak of the dedup cache
```

**Why dedup needs a full per-device set, not a high-water mark.** A
security-facing anti-replay counter only ever needs to reject counters at or
below the highest one seen, because the backend controls issuance order.
Telemetry has no such guarantee: a reading can legitimately arrive after a
later one over an unreliable link. `Dedup` therefore keeps the set of
sequences seen per device; the cost is memory proportional to readings per
device, not network reliability. That set is now capped per device (see
Findings).

**Why the pipeline never blocks on Kafka.** `Pipeline.Ingest` gives the
producer a 200ms window; if that is not enough, the reading goes to
`Spool.Write` instead of waiting longer. A slow or unreachable broker
therefore turns into "the WebSocket handler stays responsive and the spool
file grows" rather than "the whole gateway backs up".

**Why the drainer retries the spool in parallel, not one record at a time.**
The first version of `drainOnce` retried spooled readings sequentially. In
the outage benchmark that made recovery from a 60-second outage's backlog
take longer than the benchmark's drain wait, which read back as "records
lost" when they were still sitting in the spool. See Findings.

**Why verification reads Kafka back with raw partition offsets instead of a
consumer group.** The first version of the benchmark's Kafka reader used a
`kafka-go` consumer group reader, which showed 0 messages consumed even
though the messages were on the topic. See Findings.

#### Validation

- **Go tests, 7 cases, `-race` clean**: `Dedup` semantics (including that
  out-of-order sequences are legitimately admitted), the spool-on-failure and
  drain-on-recovery paths, duplicate suppression, digest order-independence,
  and a **100-trial randomized differential test** comparing the real
  `Pipeline` (backed by an in-memory fake producer) against the independent
  `ReferenceDedup` over random per-device duplicate/reorder patterns. The
  soak fix added `TestDedupCacheStaysBounded` and two related tests in
  `internal/ingest/dedup_cap_test.go`. Full output: `docs/go_test_output.txt`.
- **The replay benchmark (`loadgen`) is its own end-to-end validation**: it
  computes the reference digest and the real Kafka-materialized digest
  independently and fails loudly (non-zero exit) if they disagree.
- **The outage benchmark is self-checking**: it counts what was sent against
  what landed in Kafka and fails loudly on any mismatch in either direction
  (lost or duplicated).

#### Findings

**A consumer-group reader silently returned zero messages that were on the
topic.** The first version of the Kafka verification step used `kafka-go`'s
`Reader` with a fresh `GroupID` per run, on the theory that a brand-new
consumer group always starts from the earliest offset. The replay benchmark
reported "0 readings actually landed in kafka" against a topic that a direct
diagnostic write-then-read confirmed had messages on it. The wrong
hypothesis was that the producer was failing silently. The measurement that
discriminated it: a minimal standalone program that wrote one message and
immediately read it back with the exact same reader configuration
succeeded, so the problem was specific to timing or group coordination in
the benchmark's larger run, not the reader configuration in isolation. The
cause was reading everything through one high-level abstraction (the managed
consumer group) whose partition-assignment and rebalance handshake is
exactly the kind of timing dependency a "did everything I sent arrive" check
should not have. Fix: `ConsumeAvailable` now dials each partition directly,
reads its offset bounds, and reads exactly that range, which has no
rebalance step and is deterministic (`internal/kafkaclient/consumer.go`).

**The 60-second outage benchmark reported 1,210 "lost" records that were
still queued.** After the consumer fix, the first full 60-second outage run
reported `sent: 4250, landed in kafka: 3040`, a 1,210-record shortfall. The
wrong hypothesis was a bug in the spool-write or drain-truncate logic; a
lock-ordering issue between `Write` and `Drain` was checked first and ruled
out by inspection (both hold the same mutex for their entire critical
section). The cause was the benchmark's own drain wait: a fixed 10-second
sleep after the send phase, while `drainOnce` retried roughly 3,000
backlogged readings sequentially, one network round trip each, which
measured out to well over a minute to fully recover. Two-part fix:
`drainOnce` now retries up to 32 spooled readings concurrently, and the
benchmark polls `Spool.SizeBytes()` until it reads zero (bounded by a
5-minute cap) instead of guessing a fixed wait. After both fixes the same
scenario shows `sent: 4250, landed in kafka: 4250`, with the spool drained
in the same wall-clock window as the send phase.

**The soak found an unbounded dedup cache.** `internal/ingest.Dedup` held
one map entry per unique reading ever seen, forever, with no eviction: a
structure that a six-hour soak, let alone a real device fleet over its
lifetime, would eventually turn into an out-of-memory gateway. Fix: each
device's remembered sequences are bounded to `MaxSequencesPerDevice` (4096,
FIFO eviction) in `internal/ingest/dedup.go`, pinned by
`TestDedupCacheStaysBounded` and two related tests in
`internal/ingest/dedup_cap_test.go`.

#### Measured results

| Number | What it measures | Raw output |
|---|---|---|
| bit-identical (digest `3995b050...6a48705`) | final state after a replay 4.76% duplicated and fully reordered, 5,000 devices | `docs/replay_benchmark_output.txt` |
| 0 of 4,250 lost | records lost across a 60-second simulated broker outage | `docs/outage_benchmark_output.txt` |
| 110/sec | offered rate where spool fallback first goes nonzero (fake producer, 100/sec capacity) | `docs/loadcurve_output.txt` |
| 204,800 peak entries (vs 1,080,000 unbounded) | dedup cache size over six hours of traffic volume | `docs/soak_output.txt` |

**Replay correctness: final state bit-identical after a replay 5% duplicated
and out of order, across 5,000 simulated devices.**
`docs/replay_benchmark_output.txt`:

| | |
|---|---|
| Devices | 5,000 |
| True readings per device | 20 |
| Unique readings | 100,000 |
| Delivered (post-duplication) messages | 105,000 |
| Measured duplicate fraction of delivered traffic | 4.76% (targeting 5%; the exact ratio that makes `dup/(unique+dup) = 5%` is 4.76%, reported as measured) |
| Reordering | every device's own delivery order fully shuffled (not merely "sometimes out of order") |
| Reference-oracle digest | `3995b050d1270842e64a15b9a68cdbb11026e94915b0891b17d7535cc6a48705` |
| Kafka-materialized digest | `3995b050d1270842e64a15b9a68cdbb11026e94915b0891b17d7535cc6a48705` |
| **Result** | **bit-identical** |

The send phase over up to 400 concurrent WebSocket connections completed in
305ms; the digest comparison ran immediately after.

**Outage: 0 records lost across a 60-second simulated broker outage.**
`docs/outage_benchmark_output.txt`:

| | |
|---|---|
| Devices | 50, sending continuously at 1 reading/sec each |
| Pre-outage / outage / post-outage windows | 10s / 60s / 15s (85s total send window) |
| Sent | 4,250 |
| Landed in Kafka after drain | 4,250 |
| **Records lost** | **0** |

This does not measure how long recovery takes under an arbitrarily large
backlog, only that this backlog (roughly 3,000 readings accumulated during
the outage) fully drained. A much longer outage or a much higher device
count would need a proportionally larger drain window, which is why the
benchmark polls for an empty spool rather than assuming a fixed recovery
time. Neither the replay nor the outage benchmark measures raw throughput.

**Staged load curve: the offered rate where loss first goes nonzero.** The
gateway never permanently drops a reading that passes dedup, by design (that
is the point of the spool), so "readings lost" is 0 at every offered rate
and is not the useful number. The definition used: the offered rate at
which the pipeline could no longer produce directly within its 200ms window
and had to fall back to the spool, measured against a fixed-capacity fake
producer (not real Kafka) so the result is deterministic. The first run
(default 50-in-flight / 5ms producer, offered 2,000 to 16,000/sec) found no
threshold in range; the second (20-in-flight / 10ms, offered 500 to
6,000/sec) also found none, both because a single 200ms Ingest timeout
tolerates brief queueing even well above nominal capacity within a short
stage. The third (5-in-flight / 50ms, implied 100/sec capacity, offered 30
to 300/sec, 2s per stage) found it cleanly: 0% fallback at and below
90/sec, a clear transition at 110/sec. `docs/loadcurve_output.txt`:

| Offered rate | Fell back to spool |
|---|---|
| 90/sec | 0.00% |
| **110/sec** | **35.00%** |
| 150/sec | 85.00% |
| 300/sec | 95.83% |

**Soak: unbounded dedup cache, found, capped and pinned by a test.** "Six
hours" is the traffic volume replayed (six hours at the outage benchmark's
50-device, 1-reading/sec/device rate = 1,080,000 readings), not wall-clock
duration; it is replayed as fast as the pipeline accepts it, the same
simulated-time convention the outage benchmark uses. Measured after the fix,
`docs/soak_output.txt`:

| | |
|---|---|
| Total readings ingested | 1,080,000 |
| Entries an unbounded cache would hold | 1,080,000 |
| **Peak entries actually held (bounded)** | **204,800** (= 50 devices x 4,096 cap) |
| Regression test | `TestDedupCacheStaysBounded`: PASS |

### 2. Storefront events and segment membership

Packages: `internal/eventingest`, `internal/segment`,
`internal/simstorefront`, `internal/kafkaclient` (`storefront_producer.go`,
`storefront_consumer.go`). Commands: `cmd/eventgen`, `cmd/segmentbench`,
`cmd/throughputbench`.

A second Go edge deduplicates simulated storefront events (`page_view`,
`add_to_cart`, `purchase`, `cart_abandon`) on `event_id` into Kafka. A
consumer fans each event out to a ClickHouse event store and an incremental
Redis segment-membership engine covering 50 parameterized test segments. An
independent from-scratch recompute over the full ClickHouse event log
validates the incremental path exactly.

#### What it is and is not

- **Simulated storefront events over a simulated profile corpus.**
  `internal/simstorefront` generates events with a synthetic historical
  spread (up to 120 days in the past) so the segment predicates ("purchased
  in the last 30 days") have real history to evaluate against. That spread
  is scaffolding for the predicates, not a statement about real event volume
  over real elapsed time; the p99 lag and throughput numbers measure real
  wall-clock send/consume timing, not anything about the 120-day window.
- **A correctness-and-latency module, not a maximum-throughput module.** The
  20,000 events/sec target is a sustained offered-rate floor the edge clears
  comfortably; the benchmark does not push for a ceiling above that.
- **1M profiles is the design target, not the scale reached.** The
  full-recompute-vs-incremental diff, the p99 lag measurement, and the
  burst-drain measurement ran at up to 20,000 profiles / 110,032 events, the
  largest scale run. See Measured results and Limitations.

#### Architecture

```
proto/
  storefront.proto          # StorefrontEvent: event_id, profile_id, event_type, timestamp_ms, ...
  storefront.pb.go          # generated
internal/
  eventingest/
    dedup.go                # bounded event_id dedup for the storefront edge
    pipeline.go             # dedup -> produce for storefront events
    reference.go            # independent dedup, for the differential test
    pipeline_test.go        # unit tests + differential fuzz test
  simstorefront/
    corpus.go               # builds the synthetic storefront event corpus per profile
  segment/
    definitions.go          # the 50 test segments, the single source both engines read
    incremental.go          # Redis-backed incremental membership engine
    recompute.go            # from-scratch ClickHouse full-recompute oracle
    diff.go                 # incremental-vs-recompute set comparison
    clickhouse.go           # hand-rolled ClickHouse HTTP client (insert + aggregate query)
    segment_test.go         # segment-count/uniqueness and DiffSets unit tests
  kafkaclient/
    storefront_producer.go  # dedup -> Kafka for storefront events
    storefront_consumer.go  # streaming reader driving ClickHouse + the incremental engine
cmd/
  eventgen/main.go          # storefront edge-dedup-into-Kafka correctness benchmark
  throughputbench/main.go   # storefront edge sustained-throughput benchmark
  segmentbench/main.go      # end-to-end segment engine benchmark (lag, oracle diff, burst drain)
```

**Why the edge has its own dedup type.** A storefront event carries a
client-assigned `event_id`, a different key shape from `(device_id,
sequence)`, so `eventingest.EventDedup` shares no code with `ingest.Dedup`.

**Why the segment definitions live in their own file, imported by both
engines rather than shared through a common evaluator.** `segment.Segment`
is data (a family plus its parameters), not code. `incremental.go` and
`recompute.go` each range over the same `GenerateTestSegments()` output but
evaluate it independently: one against per-profile Redis state built up
event by event, the other with one SQL query per segment run cold against
ClickHouse. Sharing the evaluator instead of just the definitions would make
an exact-match diff between them meaningless: a bug in one shared evaluator
would still show up as "0 mismatches".

**Why membership updates are event-driven but also need a periodic sweep.**
Most of the 50 segment families become newly true only when a matching event
lands (a purchase, a cart-add), which `IncrementalEngine.ApplyEvent` handles
per event. Three families (`cart_abandon_days`, `churn_risk_days`,
`recently_active_hours`) can also become newly *false* purely because time
passed with no new event, which no event handler will ever observe.
`Finalize` re-evaluates every profile touched during the run against the
run's own latest event timestamp as `now`, which resolves those
transitions. Between events, membership in these three families can lag
reality by up to one Finalize interval.

#### Validation

- **Go tests in `internal/eventingest`**: event-id dedup semantics and
  eviction, duplicate suppression, error propagation without suppressing
  dedup, and a randomized differential test against the independent
  reference dedup. Full output: `docs/go_test_output.txt`.
- **Go tests, 3 cases in `internal/segment`**: the 50-segment count and
  ID-uniqueness pin, and two `DiffSets` tests proving the diff helper both
  reports an exact match and detects a disagreement in each direction (a
  diff helper that can only ever say "exact" would make every "50/50
  matched" result meaningless). Full output: `docs/go_test_output.txt`.
- **`eventgen` is its own end-to-end validation** for the storefront edge,
  the same shape as `loadgen`: it computes an independent reference-dedup
  digest and the real Kafka-materialized digest separately and reports
  whether they agree.
- **`segmentbench`'s full-recompute-vs-incremental diff is the primary
  correctness check for the segment engine**: for each of the 50 test
  segments, the profile-id set the incremental Redis path holds is compared,
  member for member, against an independent from-scratch ClickHouse
  recompute over the entire event log, using `DiffSets`. A segment is only
  reported as matching if the two sets are identical, not merely the same
  size.

#### Findings

**The first p99 lag measurement was 10,258,044,787 ms (about 118 days), and
it was not a lag bug in the engine.** `segmentbench` builds its corpus with
events backdated up to `history-days` (120) into the past, because the
segment predicates need real history ("purchased 3+ times in the last 30
days" needs events that are up to 30 days old). The first lag measurement
computed `time.Now() - event.TimestampMs` at the moment each event was
consumed, which is right for a live production stream where an event's
timestamp is close to now, and wrong here, where `TimestampMs` is a
deliberately backdated synthetic value: the "lag" was mostly how old the
synthetic history was, not how long the pipeline took. Fix: the benchmark
records the real wall-clock instant each event is sent through the edge
(`sentAt[event.EventId] = time.Now()`, keyed by event id) and measures lag
as `recvAt - sentAt`, the pipeline latency that matters. After the fix, p99
lag measured 72 ms at 3,000 profiles and 134 ms at 20,000 profiles, both
well under the 2-second target.

**A rerun of `segmentbench` reported `consumed: 30778 / 16559` (more events
consumed than sent) and `clickhouse event store: 0 rows`, from two separate
bugs surfacing together.** First, the Kafka topic name defaulted to a fixed
string (`storefront-events`) and `ensureTopic` only creates a topic if
absent, it never truncates one, so a second run against the same broker had
its consumer read its own new messages plus every message left over from
the previous run, corrupting the consumed count and the lag/segment numbers
with stale data. Second, `ch.CountRows` and `ch.CountDistinctProfiles`
decoded their JSON response into a `string` field, and this ClickHouse build
returns `count()`/`uniqExact()` as a bare JSON number rather than a quoted
string for these two queries (the per-row event columns elsewhere do come
back quoted), so `json.Unmarshal` left the count at its zero value, with the
decode error discarded by the original `rows, _ := ch.CountRows(ctx)` call
site. Fixes: each run gets its own uniquely suffixed Kafka topic unless
`-topic` is set explicitly, so repeated runs never share Kafka state; and
the scalar decoder targets `json.Number` (whose Kind is String, so it
accepts either a quoted string or a bare number) instead of `string`, with
the decode error surfaced instead of discarded. After both fixes, a clean
run shows `consumed: 110032 / 110032` and `clickhouse event store: 110032
rows, 20000 distinct profiles`, both exactly matching the corpus sent.

#### Measured results

Backends at measurement time: Kafka 4.3.1 (KRaft, single broker, 6
partitions), ClickHouse 25.x (single node, native WSL2 process), Redis 7.x
(single instance, native WSL2 process, port 6380). All three real, running
and reachable; none mocked.

| Number | What it measures | Target | Met | Raw output |
|---|---|---|---|---|
| 3,349 = 3,349 | edge-dedup-into-Kafka unique event ids vs independent reference | exact match | yes | `docs/eventgen_output.txt` |
| 35,791 events/sec | edge sustained throughput, 400 workers, 15s | 20,000 events/sec | yes | `docs/throughputbench_output.txt` |
| 134 ms | p99 event-to-segment lag at 20,000 profiles | under 2s | yes | `docs/segmentbench_output.txt` |
| 50 / 50 exact | segments identical to a full ClickHouse recompute | all 50 | yes | `docs/segmentbench_output.txt` |
| 10.6 ms | 10x burst backlog drain time (lag=0) | under 3 minutes | yes | `docs/segmentbench_output.txt` |
| 20,000 profiles | largest scale the segment engine ran at | 1M profiles | **no, short** | `docs/segmentbench_output.txt` |

**Edge dedup into Kafka over simulated storefront events.**
`docs/eventgen_output.txt`:

| | |
|---|---|
| Profiles | 500 |
| Unique events | 3,349 |
| Delivered (post-duplication) messages | 3,525 (target duplicate fraction 5.00%) |
| Accepted by the edge / rejected as duplicates | 3,349 / 176 |
| Kafka-materialized unique event ids | 3,349 |
| Independent reference-dedup unique event ids | 3,349 |
| **Result** | **edge-dedup-into-Kafka matches the independent reference exactly** |

**Sustained edge throughput (target 20,000 events/sec on one 16-core
machine).** `docs/throughputbench_output.txt`, 400 concurrent sender
workers, 15s offered duration:

| | |
|---|---|
| Sent (accepted + produced) | 537,264 |
| Errors | 0 |
| Elapsed | 15.011s |
| **Sustained throughput** | **35,791 events/sec** |
| Target | 20,000 events/sec |
| **Met** | **yes (1.79x the target)** |

**Segment engine: membership updated incrementally in Redis over a
ClickHouse event store (design target 1M profiles); p99 event-to-segment
lag under 2s; membership identical to a full recompute on all 50 test
segments; 10x burst backlog drained in under 3 minutes.** These share one
benchmark, `cmd/segmentbench`, run twice at increasing scale.
`docs/segmentbench_output.txt` holds the larger run:

| | Run 1 | Run 2 (reported) |
|---|---|---|
| Profiles | 3,000 | 20,000 |
| Events (corpus) | 16,559 | 110,032 |
| Consumed by segment engine | 16,559 / 16,559 | 110,032 / 110,032 |
| ClickHouse event store rows | 16,559 | 110,032 |
| ClickHouse distinct profiles | 3,000 | 20,000 |
| **p99 event-to-segment lag** | 72 ms | **134 ms** |
| Segments matched exactly vs. full recompute | 50 / 50 | **50 / 50** |
| Burst offered rate (10x sustained) | n/a shown | 7,287/sec for 8s (58,294 events) |
| **Backlog drained (lag=0)** | 11.7 ms | **10.6 ms** (cap 3m0s) |

Both runs pass the lag, oracle-diff and burst-drain checks; neither reached
the 1M-profile design target. **20,000 distinct profiles / 110,032 events is
2% of the 1M-profile target**, and the 1M target is not met at that literal
scale. The send phase at 20,000 profiles sustained 729 events/sec, well
below the 35,791 events/sec pure-edge throughput, because `segmentbench`
uses 8 send workers against a consumer that also writes to ClickHouse and
Redis in the same process, not the 400-worker configuration
`throughputbench` uses to find the edge's ceiling. Every other number in the
table came from that same unthrottled run.

### 3. Delivery webhooks, rollups and dashboard

Packages: `internal/deliverystats`, `internal/kafkaclient`
(`delivery_producer.go`, `delivery_consumer.go`, `topic.go`). Commands:
`cmd/deliverygen`, `cmd/deliveryburst`, `cmd/deliveryscale`,
`cmd/deliverythroughput`, `cmd/deliverygateway`. Dashboard:
`web/deliverydashboard`.

A third Go edge deduplicates simulated email, SMS and WhatsApp provider
delivery webhooks on `webhook_id` into Kafka. A consumer fans each webhook
out to a ClickHouse raw event log and a periodically refreshed per-minute
rollup table, both read by a small JSON API (`cmd/deliverygateway`) that a
React dashboard (`web/deliverydashboard`) polls.

#### What it is and is not

- **Simulated provider webhooks over a simulated message corpus.**
  `internal/deliverystats.Build` generates outbound messages across three
  channels (email via a simulated `sendgrid`, SMS via a simulated `twilio`,
  WhatsApp via a simulated `meta_whatsapp`), each producing a small
  realistic webhook lifecycle (`sent`, then `delivered`/`bounced`/`failed`).
- **A correctness-and-latency module for the edge, a from-scratch-recompute
  module for ClickHouse.** Dedup into Kafka is checked the same way
  `eventgen` checks the storefront edge (an independent reference dedup
  diffed against what landed). The 50M-row result is checked as a
  ClickHouse-side question, not routed back through the edge; see
  `cmd/deliveryscale`'s doc comment for why that is a deliberate scope
  choice.
- **The 50M-row bulk load bypasses Kafka and the edge on purpose.** It
  writes directly into ClickHouse's raw table to isolate its own question
  (does the rollup table's accounting tie out to an independent full
  recompute, and is a dashboard query against the rollup table faster) from
  the edge's dedup-correctness and throughput questions, which are measured
  separately. It says nothing about Kafka-to-ClickHouse throughput at 50M
  rows.
- **A separate ClickHouse database.** The module reuses the same Kafka
  broker and ClickHouse server as the segment module, against a separate
  `deliverystats` database, so a 50M-row bulk load cannot collide with, or
  slow down, the segment module's tables in `default`.
- **Measured with one run, not a median of several.** No number was tuned
  to hit a target, and no second run was tried where the first fell short
  (the dashboard-latency absolute numbers).
- **Measured on 12 logical cores.** WSL is capped to 12 logical cores by
  `~/.wslconfig` (`nproc` reflects it), and every benchmark here ran under
  that cap, not the 16 logical cores the throughput target's "one 16-core
  machine" names.

#### Architecture

```
proto/
  deliverystats.proto       # DeliveryWebhook: webhook_id, message_id, channel, status, timestamp_ms, provider
  deliverystats.pb.go       # generated
internal/
  deliverystats/
    dedup.go                # bounded webhook_id dedup, independent of eventingest's
    pipeline.go             # dedup -> produce for delivery webhooks
    reference.go            # independent dedup, for the differential test
    pipeline_test.go        # unit tests + differential fuzz test
    simwebhooks.go          # simulated email/SMS/WhatsApp webhook corpus builder
    clickhouse.go           # raw events table + rollup table + recompute/rollup queries
  kafkaclient/
    delivery_producer.go    # per-webhook and batching producers
    delivery_consumer.go    # partition-direct read-back and streaming reader
    topic.go                # shared create-topic-if-absent helper
cmd/
  deliverygateway/main.go   # standalone service: webhook HTTP receiver, Kafka producer,
                            # Kafka->ClickHouse consumer, dashboard JSON API
  deliverygen/main.go       # dedup-into-Kafka correctness benchmark
  deliverythroughput/main.go  # courtesy-capped (6 goroutines) throughput benchmark
  deliveryburst/main.go     # 10x burst backlog drain benchmark
  deliveryscale/main.go     # 50M-row bulk load, rollup-vs-recompute exactness,
                            # dashboard raw-vs-rollup p95 latency
web/deliverydashboard/      # React + TypeScript dashboard, reads /api/summary
deploy/deliverystats-*      # Dockerfiles and docker-compose, written but not built or run
```

**Why the ClickHouse raw table is ordered `(channel, status, webhook_id)`
instead of by time.** That is the edge's natural write key, but it means a
dashboard query that filters or buckets by timestamp gets no benefit from
ClickHouse's sparse primary index and has to scan the full table. That is
the deliberate reason the "raw" dashboard query is slow at scale, and the
rollup table (tiny by comparison, one row per minute/channel/status) is
fast: less data to scan, not a smarter plan over the same data. See the doc
comments in `internal/deliverystats/clickhouse.go`.

**Why the 50M-row scale benchmark writes directly to ClickHouse instead of
through Kafka.** Routing 50M rows through the courtesy-capped 6-goroutine
edge first would spend the whole time budget on Kafka throughput, a question
`deliverythroughput` already answers; the scale benchmark's question is
entirely about ClickHouse's rollup accounting once the rows exist.

**The rollup table is refreshed by a full rebuild.** `RefreshRollup`
truncates and re-aggregates the entire raw table, not incrementally. A
production deployment would more likely use a ClickHouse materialized view
or a short incremental refresh interval.

#### Validation

- **Go tests, 7 cases, `-race` clean**, the same shape as `eventingest`'s:
  webhook-id dedup semantics, retry suppression, error propagation without
  suppressing dedup, and a 100-trial randomized differential test comparing
  the real `Pipeline` (in-memory fake producer) against the independent
  `ReferenceDedup`. Full output: `docs/deliverystats_test_output.txt`.
- **`deliverygen` is its own end-to-end validation**, the same shape as
  `eventgen`: an independent reference-dedup digest and the real
  Kafka-materialized set are compared directly.
- **`deliveryscale`'s rollup-vs-recompute diff is the primary correctness
  check at scale**: for every `(channel, status)` pair, the rollup table's
  summed count is compared against an independent full-table recompute
  query, and reported exact only if every pair matches.
- **The dashboard API was checked live; the dashboard itself was not
  rendered.** `cmd/deliverygateway`'s `/api/summary`, started against the
  50M-row ClickHouse database, returned real, non-hardcoded
  per-channel/status counts on request. `web/deliverydashboard` was not
  built (`npm install`/`npm run build` were not run past dependency
  resolution) and no Playwright smoke test was run; its correctness against
  the API rests on inspection of its fetch/render code, type-checked by hand
  against the gateway's real JSON shape, not on a browser render.

#### Findings

**The first `deliverygen` run failed because the topic did not exist.** It
reported `send phase: 0 accepted, ... 4000 produce errors` and then failed
on read-back with `Unknown Topic Or Partition: the request is for a topic or
partition that does not exist on this broker`. The wrong hypothesis was a
dedup bug rejecting everything. The cause was simpler: this Kafka broker has
topic auto-creation disabled (the same single broker the segment module
uses), and every one of this module's benchmarks tried to produce into a
brand-new topic name without creating it first. Fix: call
`kafkaclient.EnsureTopic` before the send phase in `deliverygen`,
`deliverythroughput` and `deliverygateway` (`deliveryburst` already created
its topic, which is why it worked on the first run). After the fix,
`deliverygen`'s correctness run passed on the next run.

#### Measured results

Backends: Kafka 4.3.1 (KRaft, single broker, 6 partitions), ClickHouse
(single node, native WSL2 process, database `deliverystats`), 12 logical
cores under WSL.

| Number | What it measures | Target | Met | Raw output |
|---|---|---|---|---|
| 4,000 = 4,000 | edge-dedup-into-Kafka unique webhook ids vs independent reference | exact match | yes | `docs/deliverystats_dedup_output.txt` |
| 108,927 webhooks/sec | edge sustained throughput, 6 goroutines, batches of 400 | 15,000 webhooks/sec | yes, on 12 cores not 16 | `docs/deliverystats_throughput_output.txt` |
| 12 / 12 pairs, 50,000,000 = 50,000,000 | rollup counts vs full recompute over 50M rows | exact | yes | `docs/deliverystats_scale_output.txt` |
| 8,651 ms raw, 416 ms rollup p95 (20.8x) | dashboard query latency, raw vs rollup | 3.1s raw, 80ms rollup | **no on absolute numbers**, yes on rollups making it faster | `docs/deliverystats_scale_output.txt` |
| 10.4 ms | 10x burst backlog drain time (lag=0) | under 4 minutes | yes | `docs/deliverystats_burst_output.txt` |

**Deduplicating Go receiver into Kafka and ClickHouse over simulated
email/SMS/WhatsApp webhooks.** `docs/deliverystats_dedup_output.txt`:

| | |
|---|---|
| Outbound messages | 2,000 |
| Unique webhooks (sent + delivered/bounced/failed) | 4,000 |
| Delivered (post-retry) messages | 4,210 (target retry fraction 5.00%) |
| Accepted by the edge / rejected as duplicates | 4,000 / 210 |
| Kafka-materialized unique webhook ids | 4,000 |
| Independent reference-dedup unique webhook ids | 4,000 |
| **Result** | **edge-dedup-into-kafka matches the independent reference exactly** |

**Sustained throughput (target 15,000 webhooks/sec on one 16-core
machine).** `docs/deliverystats_throughput_output.txt`, 6 courtesy-capped
goroutines each batching 400 webhooks per Kafka write, 10s offered duration:

| | |
|---|---|
| Sent (accepted+produced) | 1,091,600 |
| Errors | 0 |
| Elapsed | 10.02s |
| **Sustained throughput** | **108,927 webhooks/sec** |
| Target | 15,000 webhooks/sec |
| **Met** | **yes (7.3x the target), measured on WSL's capped 12 logical cores, not 16** |

**Counts exact against a recompute over 50M events.**
`docs/deliverystats_scale_output.txt`, 50,000,000 rows bulk-loaded directly
into ClickHouse (6 courtesy-capped insert workers, 1,482,163 rows/sec, 34s
total load):

| | |
|---|---|
| Raw `delivery_events` rows | 50,000,000 |
| Rollup `delivery_rollup_minute` rows | 518,412 (1.04% the size of the raw table) |
| (channel, status) pairs matched exactly vs. full recompute | 12 / 12 |
| Recompute total / rollup total | 50,000,000 / 50,000,000 |
| **Result** | **counts exact** |

**Dashboard p95 latency (target 3.1s raw to 80ms via rollups).** Same run,
`docs/deliverystats_scale_output.txt`, 20 trials per query over a trailing
30-day window (the full loaded history):

| | Raw table (live `uniqExact` aggregate) | Rollup table (pre-aggregated) |
|---|---|---|
| p95 latency | **8,651 ms** | **416 ms** |
| min / max | 5,532 / 9,564 ms | 256 / 441 ms |
| **Speedup** | | **20.8x** |

The measured pattern has the target's shape (a live full-table aggregate is
far slower than a pre-aggregated rollup read, and rollups make the
dashboard fast), but the absolute numbers differ from the 3.1s and 80ms
targets. Raw is slower than the target (8.65s vs. 3.1s) because
`uniqExact` over a String column with no time-ordered primary key is more
expensive at 50M rows on this single-node ClickHouse than the target
implies. Rollup is slower than the target (416ms vs. 80ms) because the
rollup table itself is 518,412 rows, not small enough to answer in tens of
milliseconds on this hardware. **Met: no on the absolute numbers; yes on
the qualitative result (rollups make this dashboard meaningfully faster).**

**10x burst backlog drained in under 4 minutes.**
`docs/deliverystats_burst_output.txt`:

| | |
|---|---|
| Baseline sustained rate | 556/sec (measured over 6s unthrottled) |
| Burst offered rate | 5,556/sec (10x baseline) for 6s, 3,366 webhooks |
| **Backlog drained (lag=0)** | **10.4 ms** (cap 4m0s) |
| **Met** | **yes** |

### 4. Location pings and per-cell supply rollups

Packages: `internal/locationping`, `internal/supplyrollup`,
`internal/h3cell`, `internal/simfleet`, `internal/locationwsserver`,
`internal/kafkaclient` (`locationping_producer.go`). Commands:
`cmd/locationpingthroughputbench`, `cmd/locationpingoutagebench`,
`cmd/supplyrollupbench`.

A fourth Go edge (`internal/locationping`) reuses `internal/ingest.Dedup`
directly (the dedup key, `(device_id, sequence)`, is identical to the
device telemetry edge's), fans deduplicated location pings out to Kafka
keyed by an H3-inspired hex cell instead of by device id, and spills to disk
under backpressure with the same concurrent-drainer design as the device
telemetry edge. A consumer (`internal/supplyrollup`) applies landed pings to
a Redis-backed per-cell supply rollup, taking each device's highest-sequence
ping as its current position regardless of delivery order, validated
against an independent from-scratch recompute over the full accepted ping
set.

#### What it is and is not

- **An H3-inspired hex grid, not the real H3 library.** `internal/h3cell`
  reimplements the one property this module needs (every lat/lon maps
  deterministically to one cell id) as an axial hex grid over an
  equirectangular projection, not H3's icosahedral projection, because the
  build machine has no CGO path to libh3 (no apt package, and building it
  from source was judged out of scope for what the module needs). Cell ids
  are this package's own strings, not H3 indexes, and are not meant to be
  compared against, or interchanged with, output from `h3-go` or any other
  real H3 binding.
- **Simulated devices and a simulated outage**, the same convention as the
  device telemetry edge: real WebSocket clients, a real Kafka broker and a
  real Redis instance, with the producer told to refuse writes rather than
  the broker process being stopped.
- **Latest-sequence-wins, not first-write-wins.** `supplyrollup.
  IncrementalEngine` keeps each device's highest-applied sequence number in
  local process memory and ignores any ping at or below it, which makes
  per-cell membership well defined regardless of delivery order.
- **Single-consumer rollup.** That "latest sequence wins" bookkeeping lives
  in one consumer process's memory, not in Redis. It is correct for the
  single-consumer design built here; running multiple concurrent rollup
  consumers against the same cells would need the bookkeeping moved into
  Redis (a Lua script or a per-device `WATCH`/`MULTI`), which was not built
  because nothing in this module's targets calls for concurrent consumers.

#### Architecture

```
proto/
  locationping.proto        # LocationPing: device_id, sequence, timestamp_ms, lat, lon
  locationping.pb.go        # generated
internal/
  h3cell/
    h3cell.go               # own axial hex grid: lat/lon -> cell id, cell id -> center
    h3cell_test.go          # cell stability, distinctness, and center round-trip
  locationping/
    spool.go                # append-only length-prefixed disk overflow queue for LocationPing
    pipeline.go             # dedup (shared ingest.Dedup) -> produce; on failure -> spool
  locationwsserver/
    server.go               # one WebSocket connection per device, binary protobuf frames
  simfleet/
    corpus.go               # builds and drives the duplicated/reordered device corpus;
                            # each device steps through a ring of cells, not one fixed point
  supplyrollup/
    incremental.go          # Redis-backed per-cell membership, latest-sequence-wins
    recompute.go            # from-scratch, independent per-cell membership from all pings
    digest.go               # deterministic SHA-256 reduction for the bit-identical check
    recompute_test.go       # stale-sequence and reorder/duplication-invariance tests
  kafkaclient/
    locationping_producer.go  # produce keyed by H3 cell; partition-direct read-back
cmd/
  locationpingthroughputbench/main.go  # sustained-throughput benchmark
  locationpingoutagebench/main.go      # 60-second broker-outage benchmark
  supplyrollupbench/main.go            # end-to-end bit-identical-after-reorder benchmark
```

**Why `locationping` reuses `ingest.Dedup` directly instead of defining its
own, the way `eventingest` and `deliverystats` each do.** The dedup key
shape decides it. A storefront event carries a client-assigned `event_id`, a
different shape from `(device_id, sequence)`, which is why
`eventingest.EventDedup` shares no code with `ingest.Dedup`. A location
ping's dedup key is the identical `(device_id, sequence)` pair the telemetry
`Reading` uses, so reusing the same type is the correct design, not a
shortcut; `internal/ingest/dedup.go`'s doc comment states the general rule.

**Why the per-cell rollup keeps "latest sequence applied" in local process
memory instead of in Redis.** `IncrementalEngine.ApplyPing` decides, for
every incoming ping, whether it is newer than the position already on
record for that device. Keeping that check in Redis would need either a Lua
script or a `WATCH`/`MULTI` transaction to stay race-free under concurrent
appliers. The rollup consumer is a single goroutine applying pings one at a
time, so an in-process map gives the identical correctness guarantee
without the extra round trip. This is a single-consumer design choice, not
a statement that it scales to concurrent consumers.

**Why `Recompute` takes the whole delivered corpus, duplicates included,
rather than a deduped set.** Taking each device's highest-sequence ping as
its final position is already duplicate- and order-invariant: a duplicate of
an already-seen sequence number changes nothing, and processing order does
not matter because the comparison (`existing >= p.Sequence`) only looks at
sequence numbers, never arrival order. `TestDigestStableUnderReorderAndDuplication`
pins this directly.

#### Validation

- **Go tests, 5 cases, `-race` clean**: `h3cell`'s nearby-points-share-a-
  cell, distinct-points-differ, and center-round-trips-into-the-same-cell
  properties, and `supplyrollup`'s stale-sequence-is-ignored and
  digest-stable-under-reorder-and-duplication properties. Full output:
  `docs/go_test_output.txt`.
- **`supplyrollupbench` is its own end-to-end validation**, the same shape
  as `loadgen`: an independent `Recompute`-and-`Digest` reference and the
  real Redis-materialized rollup (built from what landed in Kafka, applied
  in Kafka's own read-back order, which is not the original send order) are
  compared directly, and the benchmark fails loudly on any digest mismatch.

#### Findings

**Topic auto-creation, again.** The first run of
`locationpingthroughputbench` reported `sent (accepted): 4916812 ... of
accepted: 0 actually produced to kafka, 4916812 fell back to the disk
spool`, the same topic-auto-creation gap the delivery-stats module hit
("this broker has topic auto-creation disabled"), reproduced because this
benchmark's topic name was new. Fixed the same way, by calling
`kafkaclient.EnsureTopic` before the send phase in all three location-ping
benchmarks.

**After that fix, the throughput benchmark measured only 558 pings/sec
against a 35,000+ target, and the cause was not a dedup or Kafka bug.** The
wrong hypothesis was that the H3-cell key computation (`fmt.Sprintf` plus a
trig call per message) was the bottleneck. The measurement that
discriminated it: an isolated single-caller, single-message `WriteMessages`
loop measured a flat 10.6ms per call regardless of payload, exactly this
producer's `BatchTimeout`. The cause was concurrency, not computation: with
`Async: false` and one message submitted per call, a goroutine's call does
not return until that message's batch flushes, so the achievable rate with
N concurrent senders is bounded by roughly N messages per `BatchTimeout`
window until enough simultaneous senders exist to fill a batch by size
instead of by timer. Measured by sender count:

| Sender goroutines | Measured rate |
|---|---|
| 6 (half of GOMAXPROCS, the courtesy-capped default worker count) | 558/sec |
| 60 | 5,547/sec |
| 256 | ~25,500/sec (three runs: 25,459 / 25,630 / 26,542) |
| 500 | ~185,000/sec (three runs: 191,228 / 182,233 / 189,936) |

The jump from 256 to 500 is nonlinear, consistent with enough concurrent
senders finally filling each partition's batch by size before its timer
fires. Because this benchmark is I/O-bound (goroutines spend nearly all
their time blocked on a channel or the network, not on CPU, confirmed by the
Kafka broker's own measured 2.9% CPU during the 500-sender run), more
goroutines than logical cores is the correct way to find this edge's
ceiling, unlike a CPU-bound benchmark where the courtesy cap matters. The
reported number uses 500 senders for that reason.

#### Measured results

Backends: Kafka 4.3.1 (KRaft, single broker, 6 partitions), Redis 7.x
(single instance, native WSL2 process, default port 6379, a dedicated
logical DB per run to avoid colliding with the segment module's Redis use),
12 logical cores under WSL.

The edge's three mechanisms (dedup on `(device_id, sequence)`, Kafka fanout
keyed by H3 cell, disk spool with a concurrent drainer under backpressure)
are exercised by all three benchmarks; `locationpingoutagebench`'s 0-lost
result is the most direct evidence the spool-and-drain path works under real
backpressure.

| Number | What it measures | Target | Met | Raw output |
|---|---|---|---|---|
| 189,936 pings/sec | edge sustained throughput, 500 senders, 20s | 35,000+ pings/sec on 12 cores | yes | `docs/locationping_throughput_output.txt` |
| 0 of 4,250 lost | pings lost across a 60-second simulated broker outage | 0 | yes | `docs/locationping_outage_output.txt` |
| bit-identical (digest `229bba6a...edaef758a`) | per-cell supply counts after a fully reordered, 4.76%-duplicated replay | SHA-256 digest match | yes | `docs/supplyrollup_output.txt` |
| all three present and exercised | dedup, cell-keyed Kafka fanout, spool + concurrent drainer | present | yes | all three files above |

**Sustained throughput (target 35,000+ pings/sec on 12 cores).**
`docs/locationping_throughput_output.txt`, 500 concurrent sender goroutines
(I/O-bound; see Findings for why this exceeds the courtesy-capped worker
count other benchmarks use), 20s offered duration:

| | |
|---|---|
| Sent (accepted) | 3,800,759 |
| Actually produced to Kafka | 3,800,759 (0 fell back to the disk spool) |
| Errors | 0 |
| Elapsed | 20.01s |
| **Sustained throughput** | **189,936 pings/sec** |
| Target | 35,000+ pings/sec |
| **Met** | **yes (5.4x the target)** |

**Outage: 0 of 4,250 pings lost across a simulated 60-second broker
outage.** `docs/locationping_outage_output.txt`, same design as
`outagebench`: 50 devices sending continuously at 1 ping/sec, 10s / 60s /
15s pre-outage / outage / post-outage windows:

| | |
|---|---|
| Sent | 4,250 |
| Landed in Kafka after drain | 4,250 |
| **Pings lost** | **0** |

**Per-cell supply counts bit-identical (SHA-256 digest match) after a fully
reordered replay.** `docs/supplyrollup_output.txt`:

| | |
|---|---|
| Devices | 2,000 |
| True pings per device (stepping through a 4-cell ring) | 20 |
| Unique pings | 40,000 |
| Delivered (post-duplication) messages | 42,000 (measured 4.76% duplicates, targeting 5%) |
| Reordering | every device's own delivery order fully shuffled, plus Kafka's own per-cell-key partitioning and partition-by-partition read-back, neither of which preserves original send order |
| Cells occupied | 400 (reference oracle and incremental rollup agree) |
| Reference-oracle digest | `229bba6adf589330fd0af97ffcdd52bc54fc00058af5e62c320c0e1edaef758a` |
| Incremental-rollup digest | `229bba6adf589330fd0af97ffcdd52bc54fc00058af5e62c320c0e1edaef758a` |
| **Result** | **bit-identical** |

## Building and running the modules

Requires Go 1.23.4 or newer (the version in `go.mod`), and a running Kafka
broker (see below for the exact local KRaft setup used for every number
above). The module path is `github.com/Manas103/event-ingestion-platform`.

```
go build ./...
go vet ./...
go test ./internal/... -v -race
```

Standing up Kafka locally (KRaft mode, no ZooKeeper):

```
curl -O https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz && cd kafka_2.13-4.3.1
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t "$CLUSTER_ID" -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties &
bin/kafka-topics.sh --create --topic telemetry --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
```

Standing up ClickHouse and Redis natively in WSL2 (the exact commands used
for the numbers above are in `docs/_start_clickhouse.sh` and
`docs/_start_redis.sh`):

```
bash docs/_start_clickhouse.sh   # binds 127.0.0.1:8123 (HTTP), data in /tmp/ch-data
bash docs/_start_redis.sh        # binds 127.0.0.1:6380, no persistence (-save "")
```

### 1. Device telemetry edge

```
# replay correctness
go run ./cmd/loadgen -devices 5000 -readings 20 -topic telemetry-replay-bench

# 60-second outage
go run ./cmd/outagebench -devices 50 -interval 1s -pre-outage 10s -outage 60s -post-outage 15s -topic telemetry-outage-bench

# staged load curve (no Kafka needed, fixed-capacity fake producer)
go run ./cmd/loadcurve -stage-duration 2s -out docs/loadcurve_output.txt

# soak (no Kafka needed, in-memory always-succeeds producer, since it measures
# dedup cache memory, not Kafka throughput)
go run ./cmd/soak -out docs/soak_output.txt

# standalone gateway service
go run ./cmd/gateway -addr :8080 -brokers 127.0.0.1:9092 -topic telemetry -spool /var/lib/gateway/spool.dat
```

Regenerating the protobuf code (only needed after editing
`proto/telemetry.proto`):

```
protoc --go_out=./proto --go_opt=paths=source_relative -I proto proto/telemetry.proto
```

### 2. Storefront events and segment membership

```
# edge-dedup correctness
go run ./cmd/eventgen -profiles 500 -dup-fraction 0.05 -topic storefront-eventgen-bench

# edge sustained throughput
go run ./cmd/throughputbench -workers 400 -duration 15s -topic storefront-throughput-bench
```

The end-to-end segment benchmark needs Kafka, ClickHouse and Redis
reachable. Each run gets its own Kafka topic automatically unless `-topic`
is set, so repeated runs never share state:

```
go run ./cmd/segmentbench -profiles 20000 -avg-events 5 -history-days 120 \
  -broker 127.0.0.1:9092 -clickhouse 127.0.0.1:8123 -redis 127.0.0.1:6380 \
  -burst-multiplier 10 -burst-seconds 8 -out docs/segmentbench_output.txt
```

### 3. Delivery webhooks, rollups and dashboard

Kafka and ClickHouse must be reachable:

```
go test ./internal/deliverystats/... -v -race
go run ./cmd/deliverygen -messages 2000 -dup-fraction 0.05 -topic deliverystats-dedup-bench -out docs/deliverystats_dedup_output.txt
go run ./cmd/deliverythroughput -duration 10s -workers 6 -topic deliverystats-throughput-bench -out docs/deliverystats_throughput_output.txt
go run ./cmd/deliveryburst -baseline-seconds 6 -burst-seconds 6 -topic deliverystats-burst -out docs/deliverystats_burst_output.txt
go run ./cmd/deliveryscale -events 50000000 -insert-workers 6 -clickhouse-db deliverystats -out docs/deliverystats_scale_output.txt
```

The standalone service (webhook receiver, Kafka->ClickHouse consumer, and
the dashboard's JSON API in one process):

```
go run ./cmd/deliverygateway -addr :8090 -broker 127.0.0.1:9092 -clickhouse 127.0.0.1:8123 -clickhouse-db deliverystats
```

The dashboard against a live gateway (not run past `npm install`'s
dependency resolution; see Validation for module 3):

```
cd web/deliverydashboard
npm install
npm run dev   # proxies /api to http://127.0.0.1:8090
```

### 4. Location pings and per-cell supply rollups

Kafka and Redis must both be reachable:

```
go run ./cmd/locationpingthroughputbench -workers 500 -duration 20s -topic location-pings-throughput-bench -out docs/locationping_throughput_output.txt
go run ./cmd/locationpingoutagebench -devices 50 -interval 1s -pre-outage 10s -outage 60s -post-outage 15s -topic location-pings-outage-bench
go run ./cmd/supplyrollupbench -devices 2000 -pings 20 -dup-fraction 0.05 -redis 127.0.0.1:6379 -redis-db 3 -topic location-pings-rollup-bench
```

Regenerating the location-ping protobuf code (only needed after editing
`proto/locationping.proto`):

```
protoc --go_out=./proto --go_opt=paths=source_relative -I proto proto/locationping.proto
```

## Limitations

Shared:

- Kafka is a single broker with no replication. The durability story for
  modules 1 and 4 rests on the spool file, not on Kafka replication, which a
  production deployment would still want in addition.
- Outages are simulated at the producer, not by stopping the real broker
  process. A real broker outage would also involve the producer's own
  connection and metadata-refresh retries, which this design does not
  specifically exercise.
- Kafka, ClickHouse and Redis ran as native WSL2 processes, not containers;
  the delivery-stats Dockerfiles and compose file were written but not built
  or run.

Device telemetry edge:

- MQTT ingest and a TimescaleDB sink were part of the original design scope
  but were not built; every target for this module is met by the
  WebSocket/Kafka path and its benchmarks without them.
- The replay and outage benchmarks are correctness checks (did the state end
  up right, did anything get lost), not throughput or latency numbers.
- The spool file has no size cap; an outage much longer than the ones tested
  would grow it without bound until disk runs out.
- No consumer-side application is included; `ConsumeAvailable` exists only
  to verify what a benchmark or test needs, not as a production consumer.

Storefront events and segment membership:

- **The 1M-profile segment-membership target was not reached.** Two runs
  were made, at 3,000 and 20,000 profiles, both passing the lag, oracle-diff
  and burst-drain checks at their own scale. 20,000 profiles is 2% of the 1M
  target. Nothing in the design is expected to stop working at larger scale
  (the incremental engine's Redis cost per event is O(segments), not
  O(profiles) or O(history), and ClickHouse's recompute path is one SQL
  aggregate per segment regardless of profile count), but that expectation
  was not verified at 1M profiles.
- The 120-day backdated history is synthetic scaffolding for the segment
  predicates, not a statement about real event volume over real elapsed
  time.
- The ClickHouse HTTP client in `internal/segment/clickhouse.go` is thin and
  hand-rolled, covering exactly the two operations this module needs (batch
  insert, aggregate query). It is not a general ClickHouse driver and has
  no connection pooling or retry logic beyond what `net/http`'s default
  transport provides.

Delivery webhooks, rollups and dashboard:

- Every result was measured with one run, not a median of several.
- The dashboard-latency absolute targets were not met: rollups are 20.8x
  faster, but raw p95 measured 8.65s (target 3.1s) and rollup p95 measured
  416ms (target 80ms) at 50M rows on this single-node ClickHouse.
- The 15,000 webhooks/sec target was measured on WSL's capped 12 logical
  cores, not the 16 its machine description names; 108,927/sec clears it
  even at the smaller core count.
- The React dashboard was not built or browser-tested; only its backing
  `/api/summary` API was verified live.
- The 50M-row bulk load bypasses Kafka and the edge; it measures ClickHouse
  rollup-vs-recompute correctness and query latency at that scale, not
  Kafka-to-ClickHouse throughput.
- The rollup table is refreshed by a full rebuild, not incrementally.

Location pings and per-cell supply rollups:

- `internal/h3cell` is not the real H3 library; its cell ids are not H3
  indexes.
- The "latest sequence wins" bookkeeping lives in one consumer process's
  memory, so the rollup supports a single consumer only.
- The reported 189,936 pings/sec uses 500 sender goroutines, well above the
  courtesy-capped half-of-GOMAXPROCS pattern other benchmarks use. See
  Findings for why that is the correct way to measure an I/O-bound
  producer's ceiling, and for the lower numbers at smaller worker counts
  (558/sec at 6, 5,547/sec at 60, ~25,500/sec at 256).

## Environment

- **Host:** Windows 11, 8 physical / 16 logical cores. No GPU is involved
  anywhere.
- **WSL2 Ubuntu 22.04.** WSL is capped to 12 logical cores by
  `~/.wslconfig` (`nproc` reflects it). The delivery-stats and location-ping
  benchmarks are recorded as running under that cap; the device telemetry
  and storefront results record the machine as 8 physical / 16 logical
  cores.
- **Go 1.23.4.**
- **Apache Kafka 4.3.1** in KRaft mode (no ZooKeeper), single broker, single
  node, running natively under **OpenJDK 17.0.20**, topics created with 6
  partitions. Topic auto-creation is disabled on this broker, so every
  benchmark creates its topic first (`kafkaclient.EnsureTopic`).
- **ClickHouse 25.x**, single node, native WSL2 process, HTTP on
  `127.0.0.1:8123`, data in `/tmp/ch-data` (`docs/_start_clickhouse.sh`).
  The segment module uses the `default` database; the delivery-stats module
  uses `deliverystats`.
- **Redis 7.x**, single instance, native WSL2 process: port 6380 with no
  persistence for the segment module (`docs/_start_redis.sh`), default port
  6379 with a dedicated logical DB per run for the location-ping module.
- **`protoc` 28.2** with `protoc-gen-go`.
- The benchmark banners inside `docs/*.txt` still print the old project name
  `device-telemetry-gateway`, and the Go test output there shows the old
  package paths (`device-telemetry-gateway/internal/...`).
