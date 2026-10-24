package locationping

import (
	"context"
	"log"
	"sync"
	"time"

	"device-telemetry-gateway/internal/ingest"
	pb "device-telemetry-gateway/proto"
)

// Producer is the narrow interface Pipeline needs from a Kafka writer.
type Producer interface {
	Produce(ctx context.Context, p *pb.LocationPing) error
}

// Pipeline is the location-ping ingest path: dedup on (device_id,
// sequence) using the shared ingest.Dedup, then hand off to Kafka; spill to
// disk under backpressure exactly like internal/ingest.Pipeline, because a
// lost ping and a lost reading are the same failure mode.
type Pipeline struct {
	dedup    *ingest.Dedup
	spool    *Spool
	producer Producer
}

func NewPipeline(dedup *ingest.Dedup, spool *Spool, producer Producer) *Pipeline {
	return &Pipeline{dedup: dedup, spool: spool, producer: producer}
}

func (p *Pipeline) Ingest(ctx context.Context, ping *pb.LocationPing) error {
	if !p.dedup.Admit(ping.DeviceId, ping.Sequence) {
		return nil
	}

	produceCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := p.producer.Produce(produceCtx, ping); err != nil {
		if spoolErr := p.spool.Write(ping); spoolErr != nil {
			log.Printf("locationping: spool write failed after produce failure (produce error: %v): %v", err, spoolErr)
			return spoolErr
		}
		return nil
	}
	return nil
}

// RunDrainer periodically retries every spooled ping against the producer
// until ctx is cancelled, with the same bounded concurrency as
// ingest.Pipeline.RunDrainer.
func (p *Pipeline) RunDrainer(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.drainOnce(ctx)
		}
	}
}

const drainConcurrency = 32

func (p *Pipeline) drainOnce(ctx context.Context) {
	records, err := p.spool.Drain()
	if err != nil {
		log.Printf("locationping: spool drain failed: %v", err)
		return
	}

	sem := make(chan struct{}, drainConcurrency)
	var wg sync.WaitGroup
	for _, ping := range records {
		wg.Add(1)
		sem <- struct{}{}
		go func(ping *pb.LocationPing) {
			defer wg.Done()
			defer func() { <-sem }()

			produceCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			err := p.producer.Produce(produceCtx, ping)
			cancel()
			if err != nil {
				if spoolErr := p.spool.Write(ping); spoolErr != nil {
					log.Printf("locationping: re-spool failed for %s/%d: %v", ping.DeviceId, ping.Sequence, spoolErr)
				}
			}
		}(ping)
	}
	wg.Wait()
}
