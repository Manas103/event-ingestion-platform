// cmd/locationpingthroughputbench is the sustained-throughput claim's exact
// measurement for the location-ping extension: hammer
// internal/locationping.Pipeline directly (dedup, then a real Kafka
// produce keyed by H3 cell) with as much concurrency as this machine's
// cores support, for a fixed duration, and report pings/sec actually
// sustained. Direct in-process calls rather than a WebSocket round trip
// measure the edge's own code path, the same precedent cmd/throughputbench
// set for the storefront edge.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/kafkaclient"
	"github.com/Manas103/event-ingestion-platform/internal/locationping"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "location-pings-throughput-bench", "Kafka topic")
	duration := flag.Duration("duration", 20*time.Second, "how long to sustain the offered load")
	workers := flag.Int("workers", 0, "concurrent sender goroutines (0 = half of GOMAXPROCS, per this project's resource-courtesy rule)")
	spoolPath := flag.String("spool", "/tmp/locationpingthroughputbench-spool.dat", "backpressure spool path (should stay empty if Kafka keeps up)")
	outFile := flag.String("out", "", "path to also write the full report to")
	flag.Parse()

	if *workers <= 0 {
		*workers = runtime.GOMAXPROCS(0) / 2
		if *workers < 1 {
			*workers = 1
		}
	}

	var out *os.File
	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			log.Fatalf("create out file: %v", err)
		}
		defer f.Close()
		out = f
	}
	w := func(format string, args ...interface{}) {
		s := fmt.Sprintf(format, args...)
		fmt.Print(s)
		if out != nil {
			fmt.Fprint(out, s)
		}
	}

	if err := kafkaclient.EnsureTopic(*broker, *topic, 6); err != nil {
		log.Fatalf("ensure topic: %v", err)
	}

	realProducer := kafkaclient.NewLocationPingProducer(*broker, *topic)
	defer realProducer.Close()
	producer := &countingProducer{inner: realProducer}

	spool, err := locationping.NewSpool(*spoolPath)
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	defer spool.Close()

	pipeline := locationping.NewPipeline(ingest.NewDedup(), spool, producer)

	w("=== device-telemetry-gateway location-ping edge sustained throughput benchmark ===\n")
	w("workers: %d (logical cores available: %d), offered duration: %s\n", *workers, runtime.NumCPU(), *duration)

	ctx := context.Background()
	var sent, errs int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	start := time.Now()

	for wk := 0; wk < *workers; wk++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var seq uint64
			deviceID := fmt.Sprintf("bench-dev-%d", workerID)
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq++
				p := &pb.LocationPing{
					DeviceId:    deviceID,
					Sequence:    seq,
					TimestampMs: time.Now().UnixMilli(),
					Lat:         40.70 + 0.001*float64(seq%50),
					Lon:         -74.00 + 0.001*float64(seq%50),
				}
				if err := pipeline.Ingest(ctx, p); err != nil {
					atomic.AddInt64(&errs, 1)
				} else {
					atomic.AddInt64(&sent, 1)
				}
			}
		}(wk)
	}

	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	spoolSize, _ := spool.SizeBytes()
	produced := atomic.LoadInt64(&producer.produced)
	fellBackToSpool := atomic.LoadInt64(&producer.failed)
	rate := float64(produced) / elapsed.Seconds()
	w("sent (accepted): %d, errors: %d, elapsed: %s\n", sent, errs, elapsed)
	w("of accepted: %d actually produced to kafka, %d fell back to the disk spool (%d bytes left unsent at cutoff)\n", produced, fellBackToSpool, spoolSize)
	w("RESULT: sustained throughput actually landed in kafka: %.0f pings/sec\n", rate)
}

// countingProducer wraps the real Kafka producer to separate "accepted by
// the pipeline" from "actually produced to Kafka": under enough offered
// load the Kafka writer's batch queue fills and Produce starts returning an
// error, at which point the pipeline spools to disk instead. Counting only
// successful Produce calls is what keeps the reported throughput honest
// about what this machine can sustain into Kafka, rather than crediting
// disk-buffered backlog as "sustained".
type countingProducer struct {
	inner    *kafkaclient.LocationPingProducer
	produced int64
	failed   int64
}

func (p *countingProducer) Produce(ctx context.Context, ping *pb.LocationPing) error {
	if err := p.inner.Produce(ctx, ping); err != nil {
		atomic.AddInt64(&p.failed, 1)
		return err
	}
	atomic.AddInt64(&p.produced, 1)
	return nil
}
