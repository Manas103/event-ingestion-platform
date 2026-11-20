#!/usr/bin/env python3
"""Summarize one kind_failover.sh run from its raw logs.

Recovery is read off the devices' own ack timeline (acks per 100 ms): the
pre-fault rate is the mean over the 10 s before the fault; recovered is the
first moment after the fault from which the 1 s rolling ack rate stays at or
above 90% of it for 3 s. The consumer stall is the longest run of 100 ms
buckets with nothing applied in the 30 s after the fault.
"""
import json
import re
import statistics
import sys
from pathlib import Path

out = Path(sys.argv[1])
prefix = sys.argv[2] if len(sys.argv) > 2 else ""  # results/aws keeps <experiment>-devicesim.log etc.
name = prefix.rstrip("-") or out.name
dev_log = (out / f"{prefix}devicesim.log").read_text()
ver_log = (out / f"{prefix}verify.log").read_text()
fault_txt = (out / f"{prefix}fault.txt").read_text()

rep = json.loads(re.search(r"^REPORT_JSON (.*)$", dev_log, re.M).group(1))
fault_ms = int(re.search(r"FAULT_MS=(\d+)", fault_txt).group(1))
t0, bucket = rep["t0_unix_ms"], rep["bucket_ms"]
acks = rep["ack_timeline"]
f = (fault_ms - t0) // bucket  # fault bucket index
per_s = 1000 // bucket

def rate(i):  # acks/s over the 1 s window ending at bucket i
    w = acks[max(0, i - per_s + 1): i + 1]
    return sum(w) * per_s / max(1, len(w))

pre = acks[max(0, f - 10 * per_s): f]
pre_rate = sum(pre) * per_s / max(1, len(pre))
recovered_at = None
dip = min((rate(i) for i in range(f, min(len(acks), f + 30 * per_s))), default=0)
for i in range(f, len(acks) - 3 * per_s):
    if all(rate(j) >= 0.9 * pre_rate for j in range(i, i + 3 * per_s)):
        recovered_at = i
        break

gaps = sorted(rep["reconnect_gaps_ms"] or [])
def pct(xs, p):
    return xs[int((len(xs) - 1) * p / 100)] if xs else 0

print(f"scenario: {name}")
print(f"fault: {fault_txt.strip().splitlines()[0]}")
print(f"fault at t+{(fault_ms - t0) / 1000:.1f} s of a {rep['elapsed_ms'] / 1000:.1f} s run; "
      f"{rep['devices']} devices x {rep['readings_per_device']} readings at {rep['rate_per_device']}/s each")
print(f"frames: sent {rep['sent']}, resent after reconnect {rep['resent']}, acked {rep['acked']}; "
      f"disconnects {rep['disconnects']}, failed redials {rep['dial_failures']}")
print(f"ack rate: {pre_rate:.0f}/s before the fault, lowest 1 s window after it {dip:.0f}/s")
if rep["disconnects"] or name != "baseline":
    if recovered_at is None:
        print("recovered to 90% of the pre-fault ack rate: not within the run")
    else:
        print(f"recovered to 90% of the pre-fault ack rate (held 3 s): {(recovered_at - f) * bucket / 1000:.1f} s after the fault")
if gaps:
    print(f"device reconnect gap (connection lost to first ack after reconnecting), n={len(gaps)}: "
          f"p50 {pct(gaps, 50):.0f} ms, p99 {pct(gaps, 99):.0f} ms, max {gaps[-1]:.0f} ms")

tl = json.loads(re.search(r"^TIMELINE_JSON (.*)$", ver_log, re.M).group(1))["applied_per_100ms"]
fb = fault_ms // 100
end_b = (t0 + rep["elapsed_ms"]) // 100  # stop at the end of the run, not at idle time after it
run = longest = 0
for b in range(fb, min(fb + 300, end_b)):
    if int(tl.get(str(b), 0)) == 0:
        run += 1
        longest = max(longest, run)
    else:
        run = 0
print(f"consumer group: longest stretch with nothing applied in the 30 s after the fault (while devices were still sending): {longest * 100} ms")
for line in ver_log.splitlines():
    if line.startswith(("expected:", "redis store:", "applier counters:", "kafka log:", "end-to-end", "  of which", "  Kafka", "RESULT:")):
        print(line)
for extra in ("gateway_stats.txt", "pods.txt", f"{prefix}containers.txt"):
    if prefix and extra == "pods.txt" or not (out / extra).exists():
        continue
    print("after the run:")
    print((out / extra).read_text().strip())
