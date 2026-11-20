package ingest

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// Producer is the narrow interface Pipeline needs from a Kafka writer. Tests
// and the outage benchmark substitute a fault-injecting implementation
// without touching the pipeline logic itself.
type Producer interface {
	Produce(ctx context.Context, r *pb.Reading) error
}

// Counters are the pipeline's running totals since start, read by the admin
// API. All fields are updated atomically.
type Counters struct {
	Received   atomic.Uint64
	Duplicates atomic.Uint64
	Produced   atomic.Uint64
	Spooled    atomic.Uint64
	Drained    atomic.Uint64
}

// Pipeline is the ingest path: dedup, then hand off to Kafka; if Kafka
// cannot take it right now, spill to disk and let the drainer retry later.
// Nothing here ever silently drops a reading that passed dedup.
type Pipeline struct {
	dedup    *Dedup
	spool    *Spool
	producer Producer
	Counters Counters

	// ProduceTimeout bounds one produce attempt before the reading is spooled
	// instead. 200 ms suits a single local broker with acks=1; with acks=all
	// across brokers on different nodes it needs more headroom.
	ProduceTimeout time.Duration
}

func NewPipeline(dedup *Dedup, spool *Spool, producer Producer) *Pipeline {
	return &Pipeline{dedup: dedup, spool: spool, producer: producer, ProduceTimeout: 200 * time.Millisecond}
}

// Dedup exposes the edge dedup for the admin API's counters.
func (p *Pipeline) Dedup() *Dedup { return p.dedup }

// Spool exposes the spool for the admin API's counters.
func (p *Pipeline) Spool() *Spool { return p.spool }

// Ingest is called once per Reading arriving over a device's WebSocket
// connection. It returns quickly: a Kafka failure never blocks the caller,
// it spills to disk and returns. A nil return means the reading is durable
// (produced, spooled, or a duplicate of one that was), which is the condition
// the edge acks on. On a non-nil return nothing kept the reading, so its dedup
// entry is rolled back and the device's resend will be admitted.
func (p *Pipeline) Ingest(ctx context.Context, r *pb.Reading) error {
	p.Counters.Received.Add(1)
	if !p.dedup.Admit(r.DeviceId, r.Sequence) {
		p.Counters.Duplicates.Add(1)
		return nil // exact duplicate of an already-forwarded reading; dropped by design
	}

	produceCtx, cancel := context.WithTimeout(ctx, p.ProduceTimeout)
	defer cancel()
	if err := p.producer.Produce(produceCtx, r); err != nil {
		if spoolErr := p.spool.Write(r); spoolErr != nil {
			p.dedup.Forget(r.DeviceId, r.Sequence)
			log.Printf("ingest: spool write failed after produce failure (produce error: %v): %v", err, spoolErr)
			return spoolErr
		}
		p.Counters.Spooled.Add(1)
		return nil
	}
	p.Counters.Produced.Add(1)
	return nil
}

// RunDrainer periodically retries every spooled reading against the
// producer until ctx is cancelled. A reading that still cannot be produced
// (the outage is still ongoing) goes straight back into the spool, so a
// reading is never lost between drain attempts.
func (p *Pipeline) RunDrainer(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.DrainOnce(ctx)
		}
	}
}

// drainConcurrency bounds how many spooled readings are retried against the
// producer at once. A long outage can leave thousands of readings queued;
// retrying them one at a time, each paying a network round trip, made
// recovery take far longer than the outage itself. Retrying in parallel
// keeps recovery time proportional to the backlog divided by concurrency,
// not to the backlog alone.
const drainConcurrency = 32

// DrainOnce retries every spooled reading now and returns how many were
// produced and how many went back to the spool. The spool file is only
// rewritten after the retries finish (Spool.DrainWith), so a crash mid-drain
// loses nothing.
func (p *Pipeline) DrainOnce(ctx context.Context) (drained, respooled int) {
	drained, respooled, err := p.spool.DrainWith(func(records []*pb.Reading) []*pb.Reading {
		sem := make(chan struct{}, drainConcurrency)
		var (
			wg     sync.WaitGroup
			mu     sync.Mutex
			failed []*pb.Reading
		)
		for _, r := range records {
			wg.Add(1)
			sem <- struct{}{}
			go func(r *pb.Reading) {
				defer wg.Done()
				defer func() { <-sem }()
				produceCtx, cancel := context.WithTimeout(ctx, p.ProduceTimeout)
				err := p.producer.Produce(produceCtx, r)
				cancel()
				if err != nil {
					mu.Lock()
					failed = append(failed, r)
					mu.Unlock()
				}
			}(r)
		}
		wg.Wait()
		return failed
	})
	if err != nil {
		log.Printf("ingest: spool drain failed: %v", err)
	}
	p.Counters.Drained.Add(uint64(drained))
	return drained, respooled
}
