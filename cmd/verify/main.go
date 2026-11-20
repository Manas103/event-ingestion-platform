// verify is the end-to-end check after a devicesim run: it rebuilds the same
// deterministic corpus, computes the expected final state with the
// independent reference dedup, then compares it with what the appliers
// stored in Redis (lost, extra, wrong value, digest) and with what is in the
// Kafka log (how many duplicates at-least-once delivery put there). It also
// reports the sampled end-to-end latency and the applied-per-100ms timeline.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Manas103/event-ingestion-platform/internal/applier"
	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/kafkaclient"
	"github.com/Manas103/event-ingestion-platform/internal/simdevice"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

func main() {
	devices := flag.Int("devices", 1000, "must match devicesim")
	readings := flag.Int("readings", 600, "must match devicesim")
	dup := flag.Float64("dup-fraction", 0.05, "must match devicesim")
	seed := flag.Int64("seed", 20261120, "must match devicesim")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	brokers := flag.String("brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "telemetry", "kafka topic")
	settle := flag.Duration("settle", 3*time.Minute, "max wait for the appliers to catch up")
	out := flag.String("out", "", "write the applied timeline (JSON) to this file, or \"stdout\"")
	flag.Parse()
	ctx := context.Background()

	streams := simdevice.BuildReplayCorpus(*devices, *readings, *dup, *seed)
	expected := ingest.ReferenceDedup(simdevice.AllUnique(streams))
	fmt.Printf("expected: %d unique readings from %d devices\n", len(expected), *devices)

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	// wait until stats:new reaches the expected count, or stops moving
	deadline := time.Now().Add(*settle)
	var last int64 = -1
	stillFor := 0
	for time.Now().Before(deadline) {
		n, _ := rdb.Get(ctx, "stats:new").Int64()
		if n >= int64(len(expected)) {
			break
		}
		if n == last {
			stillFor++
			if stillFor >= 20 { // 10 s without progress
				break
			}
		} else {
			stillFor = 0
		}
		last = n
		time.Sleep(500 * time.Millisecond)
	}

	var stored []*pb.Reading
	wrong := 0
	byKey := map[string]float64{}
	for _, e := range expected {
		byKey[e.DeviceId+"/"+strconv.FormatUint(e.Sequence, 10)] = e.Value
	}
	for _, s := range streams {
		h, err := rdb.HGetAll(ctx, applier.DeviceKey(s.DeviceID)).Result()
		if err != nil {
			log.Fatalf("redis: %v", err)
		}
		for f, v := range h {
			seq, _ := strconv.ParseUint(f, 10, 64)
			val, err := applier.DecodeValue(v)
			if err != nil {
				wrong++
				continue
			}
			stored = append(stored, &pb.Reading{DeviceId: s.DeviceID, Sequence: seq, Value: val})
		}
	}
	missing, extra := 0, 0
	seen := map[string]bool{}
	for _, r := range stored {
		k := r.DeviceId + "/" + strconv.FormatUint(r.Sequence, 10)
		seen[k] = true
		want, ok := byKey[k]
		if !ok {
			extra++
		} else if want != r.Value {
			wrong++
		}
	}
	for k := range byKey {
		if !seen[k] {
			missing++
		}
	}
	match := string(ingest.FinalStateDigest(expected)) == string(ingest.FinalStateDigest(stored))
	newN, _ := rdb.Get(ctx, "stats:new").Int64()
	dupN, _ := rdb.Get(ctx, "stats:dup").Int64()
	fmt.Printf("redis store: %d readings stored, %d missing, %d extra, %d wrong value\n", len(stored), missing, extra, wrong)
	fmt.Printf("applier counters: %d applied new, %d applied again (redelivered or duplicated in the log)\n", newN, dupN)

	logged, err := kafkaclient.ConsumeAllFrom(ctx, kafkaclient.SplitBrokers(*brokers), *topic)
	if err != nil {
		fmt.Printf("kafka log: could not read (%v)\n", err)
	} else {
		distinct := map[string]bool{}
		for _, r := range logged {
			distinct[r.DeviceId+"/"+strconv.FormatUint(r.Sequence, 10)] = true
		}
		fmt.Printf("kafka log: %d messages, %d distinct readings, %d duplicate messages (%.3f%%)\n",
			len(logged), len(distinct), len(logged)-len(distinct),
			100*float64(len(logged)-len(distinct))/float64(max(1, len(logged))))
	}

	lags, _ := rdb.LRange(ctx, "lag", 0, -1).Result()
	var all, fresh, again, logToApply []float64
	for _, l := range lags {
		f := strings.Split(l, ",")
		if len(f) != 4 {
			continue
		}
		total, _ := strconv.ParseFloat(f[0], 64)
		l2a, _ := strconv.ParseFloat(f[1], 64)
		all = append(all, total)
		logToApply = append(logToApply, l2a)
		if f[3] == "1" {
			fresh = append(fresh, total)
		} else {
			again = append(again, total)
		}
	}
	line := func(name string, xs []float64) {
		sort.Float64s(xs)
		pct := func(p float64) float64 {
			if len(xs) == 0 {
				return 0
			}
			return xs[int(float64(len(xs)-1)*p/100)]
		}
		fmt.Printf("%s (%d samples): p50=%.0f ms p99=%.0f ms p99.9=%.0f ms max=%.0f ms\n",
			name, len(xs), pct(50), pct(99), pct(99.9), pct(100))
	}
	line("end-to-end latency, first send to applied, every sampled message", all)
	line("  of which first application of a reading", fresh)
	line("  of which re-application (duplicate in the log or redelivery)", again)
	line("  Kafka timestamp to applied (consumer side only)", logToApply)

	// raw samples, for anyone who wants to look at the slow ones
	if *out == "stdout" {
		fmt.Printf("LAG_SAMPLES %s\n", strings.Join(lags, ";"))
	}

	if *out != "" {
		keys, _ := rdb.Keys(ctx, "tl:*").Result()
		tl := map[string]int64{}
		for _, k := range keys {
			v, _ := rdb.Get(ctx, k).Int64()
			tl[strings.TrimPrefix(k, "tl:")] = v
		}
		b, _ := json.Marshal(map[string]any{"applied_per_100ms": tl})
		if *out == "stdout" {
			fmt.Printf("TIMELINE_JSON %s\n", b)
		} else {
			_ = os.WriteFile(*out, b, 0o644)
		}
	}

	ok := match && missing == 0 && extra == 0 && wrong == 0
	fmt.Printf("RESULT: zero lost, zero extra, final state digest identical to the reference: %v\n", ok)
	if !ok {
		os.Exit(1)
	}
}
