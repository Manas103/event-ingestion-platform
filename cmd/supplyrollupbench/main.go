// supplyrollupbench is the reproducible measurement behind this extension's
// "per-cell supply counts bit-identical after a fully reordered replay"
// claim. It starts a real location-ping edge (real WebSocket server, real
// dedup, real Kafka producer keyed by H3 cell against the already-running
// broker), drives numDevices simulated devices that each move through a
// small ring of cells, with a delivery order that is duplicated and fully
// shuffled per device, reads everything back from Kafka with a real
// consumer and applies it to a real Redis-backed IncrementalEngine in the
// order Kafka happens to hand it back (itself not the devices' original
// send order, since Kafka's per-cell keying and partition-by-partition
// consumption reorders relative to it), then diffs the result against an
// independently computed reference digest.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/kafkaclient"
	"github.com/Manas103/event-ingestion-platform/internal/locationping"
	"github.com/Manas103/event-ingestion-platform/internal/locationwsserver"
	"github.com/Manas103/event-ingestion-platform/internal/simfleet"
	"github.com/Manas103/event-ingestion-platform/internal/supplyrollup"
)

func main() {
	numDevices := flag.Int("devices", 2000, "number of simulated devices")
	pingsPerDevice := flag.Int("pings", 20, "true pings per device")
	dupFraction := flag.Float64("dup-fraction", 0.05, "fraction of the delivered stream that is duplicate traffic")
	concurrency := flag.Int("concurrency", 400, "max concurrent device connections")
	broker := flag.String("broker", "127.0.0.1:9092", "kafka broker address")
	topic := flag.String("topic", "location-pings-rollup-bench", "kafka topic (should be empty/fresh for a clean measurement)")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	redisPrefix := flag.Int("redis-db", 3, "redis logical db to isolate this benchmark's keys")
	spoolPath := flag.String("spool", "/tmp/supplyrollupbench-spool.dat", "backpressure spool path")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, DB: *redisPrefix})
	defer rdb.Close()
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		log.Fatalf("redis flushdb: %v", err)
	}

	if err := kafkaclient.EnsureTopic(*broker, *topic, 6); err != nil {
		log.Fatalf("ensure topic: %v", err)
	}

	producer := kafkaclient.NewLocationPingProducer(*broker, *topic)
	defer producer.Close()

	spool, err := locationping.NewSpool(*spoolPath)
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	defer spool.Close()

	pipeline := locationping.NewPipeline(ingest.NewDedup(), spool, producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipeline.RunDrainer(ctx, 500*time.Millisecond)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: locationwsserver.NewServer(pipeline).Handler()}
	go srv.Serve(listener)
	defer srv.Close()

	wsURL := fmt.Sprintf("ws://%s/ingest", listener.Addr())
	fmt.Printf("location-ping edge listening on %s, kafka broker %s, topic %q\n", listener.Addr(), *broker, *topic)

	streams := simfleet.BuildReplayCorpus(*numDevices, *pingsPerDevice, *dupFraction, 20261024)

	totalUnique := *numDevices * *pingsPerDevice
	totalDelivered := 0
	for _, s := range streams {
		totalDelivered += len(s.Delivery)
	}
	measuredDupFraction := float64(totalDelivered-totalUnique) / float64(totalDelivered)
	fmt.Printf("corpus: %d devices x %d pings = %d unique pings; %d delivered (%.2f%% duplicates, targeting %.2f%%)\n",
		*numDevices, *pingsPerDevice, totalUnique, totalDelivered, measuredDupFraction*100, *dupFraction*100)

	start := time.Now()
	if err := simfleet.Send(ctx, wsURL, streams, *concurrency); err != nil {
		log.Fatalf("send: %v", err)
	}
	fmt.Printf("send complete in %s over up to %d concurrent WebSocket connections\n", time.Since(start), *concurrency)

	time.Sleep(2 * time.Second)

	wantByCell := supplyrollup.Recompute(simfleet.AllDelivered(streams))
	wantDigest := supplyrollup.Digest(wantByCell)
	fmt.Printf("reference oracle: %d cells occupied, digest %x\n", len(wantByCell), wantDigest)

	landed, err := kafkaclient.ConsumeAvailableLocationPings(context.Background(), *broker, *topic)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	fmt.Printf("kafka consumer: %d pings actually landed\n", len(landed))

	engine := supplyrollup.NewIncrementalEngine(rdb)
	for _, ping := range landed {
		if err := engine.ApplyPing(context.Background(), ping); err != nil {
			log.Fatalf("apply ping: %v", err)
		}
	}

	gotByCell, err := engine.Snapshot(context.Background())
	if err != nil {
		log.Fatalf("snapshot: %v", err)
	}
	gotDigest := supplyrollup.Digest(gotByCell)
	fmt.Printf("incremental rollup: %d cells occupied, digest %x\n", len(gotByCell), gotDigest)

	match := string(wantDigest) == string(gotDigest)
	fmt.Printf("\nRESULT: per-cell supply counts bit-identical after a %.2f%%-duplicated, fully reordered replay: %v\n", measuredDupFraction*100, match)
	if !match {
		log.Fatalf("DIGESTS DO NOT MATCH")
	}
}
