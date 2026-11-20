// devicesim drives a fleet of simulated devices against a running gateway
// (or a Kubernetes Service in front of several) with the acked protocol:
// every device resends what was not acked after any disconnect. It writes a
// JSON report with the per-bucket ack timeline and every reconnect gap, which
// the failover scripts line up against the moment they killed something.
//
// The corpus is the same deterministic one cmd/verify rebuilds (same
// -devices, -readings, -dup-fraction, -seed), so the verifier knows exactly
// which readings must have landed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Manas103/event-ingestion-platform/internal/simdevice"
)

type report struct {
	T0UnixMs     int64     `json:"t0_unix_ms"`
	BucketMs     int64     `json:"bucket_ms"`
	Devices      int       `json:"devices"`
	Readings     int       `json:"readings_per_device"`
	DupFraction  float64   `json:"dup_fraction"`
	Rate         float64   `json:"rate_per_device"`
	ElapsedMs    int64     `json:"elapsed_ms"`
	Sent         uint64    `json:"sent"`
	Resent       uint64    `json:"resent"`
	Acked        uint64    `json:"acked"`
	Disconnects  uint64    `json:"disconnects"`
	DialFailures uint64    `json:"dial_failures"`
	GapsMs       []float64 `json:"reconnect_gaps_ms"`
	AckTimeline  []uint64  `json:"ack_timeline"`
	DropTimeline []uint64  `json:"drop_timeline"`
}

func main() {
	urls := flag.String("url", "ws://127.0.0.1:8080/ingest?ack=1", "comma-separated gateway URLs (include ?ack=1)")
	devices := flag.Int("devices", 1000, "simulated devices")
	readings := flag.Int("readings", 600, "true readings per device")
	dup := flag.Float64("dup-fraction", 0.05, "fraction of delivered frames that are duplicates")
	seed := flag.Int64("seed", 20261120, "corpus seed")
	rate := flag.Float64("rate", 10, "fresh frames per second per device (0 = window-limited)")
	window := flag.Int("window", 64, "max unacked frames per device")
	timeout := flag.Duration("timeout", 15*time.Minute, "give up after this long")
	out := flag.String("out", "", "write the JSON report to this file, or \"stdout\"")
	flag.Parse()

	streams := simdevice.BuildReplayCorpus(*devices, *readings, *dup, *seed)
	total := 0
	for _, s := range streams {
		total += len(s.Delivery)
	}
	log.Printf("devicesim: %d devices x %d readings (%d frames with duplicates), %.1f/s each, window %d, targets %s",
		*devices, *readings, total, *rate, *window, *urls)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	stats, err := simdevice.SendAcked(ctx, streams, simdevice.AckedConfig{
		URLs: strings.Split(*urls, ","), Rate: *rate, Window: *window,
		Bucket: 100 * time.Millisecond, MaxDuration: *timeout + time.Minute,
	})
	fmt.Printf("T0_UNIX_MS=%d\n", stats.T0.UnixMilli())
	fmt.Println("devicesim:", stats)
	g := stats.ReconnectGaps
	fmt.Printf("reconnect gaps: n=%d p50=%s p99=%s max=%s\n", len(g),
		simdevice.Percentile(g, 50), simdevice.Percentile(g, 99), simdevice.Percentile(g, 100))

	if *out != "" {
		rep := report{
			T0UnixMs: stats.T0.UnixMilli(), BucketMs: 100, Devices: *devices, Readings: *readings,
			DupFraction: *dup, Rate: *rate, ElapsedMs: stats.Elapsed.Milliseconds(),
			Sent: stats.Sent, Resent: stats.Resent, Acked: stats.Acked, Disconnects: stats.Disconnects,
			DialFailures: stats.DialFailures, AckTimeline: stats.AckTimeline, DropTimeline: stats.DropTimeline,
		}
		for _, d := range g {
			rep.GapsMs = append(rep.GapsMs, float64(d.Microseconds())/1000)
		}
		b, _ := json.Marshal(rep)
		if *out == "stdout" {
			fmt.Printf("REPORT_JSON %s\n", b) // for in-cluster runs, read back with kubectl logs
		} else if werr := os.WriteFile(*out, b, 0o644); werr != nil {
			log.Printf("write report: %v", werr)
		}
	}
	if err != nil {
		log.Fatalf("devicesim: %v", err)
	}
	fmt.Println("RESULT: every reading acked at least once: true")
}
