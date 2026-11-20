// Package applier is the partitioned consumer side of the platform: members
// of one Kafka consumer group each own a subset of the topic's partitions and
// apply every reading to a Redis store idempotently, so the at-least-once
// delivery the edge and the group rebalances produce still ends in exactly
// one stored value per (device_id, sequence).
//
// Store layout (one Redis hash per device, so a device's readings live on one
// key and one partition's member writes them):
//
//	r:{device_id}   field = sequence, value = float64 bits as decimal
//	stats:new       readings stored for the first time
//	stats:dup       readings already stored (redelivery or a duplicate in the log)
//	tl:{unix_ms/100} readings applied in that 100 ms bucket (all members)
//	lag             sampled latency: "total_ms,log_to_apply_ms,partition,new"
package applier

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// LagSampleEvery keeps one end-to-end latency sample per this many readings.
const LagSampleEvery = 10

// Store applies batches of readings to Redis.
type Store struct {
	rdb *redis.Client
	n   uint64
}

func NewStore(rdb *redis.Client) *Store { return &Store{rdb: rdb} }

// DeviceKey is the hash holding one device's readings.
func DeviceKey(deviceID string) string { return "r:" + deviceID }

// EncodeValue stores a float64 exactly.
func EncodeValue(v float64) string { return strconv.FormatUint(math.Float64bits(v), 10) }

// DecodeValue is the inverse of EncodeValue.
func DecodeValue(s string) (float64, error) {
	u, err := strconv.ParseUint(s, 10, 64)
	return math.Float64frombits(u), err
}

// Item is one fetched reading plus where it came from in the log.
type Item struct {
	Reading   *pb.Reading
	Partition int
	LogTime   time.Time // Kafka message timestamp: when the gateway produced it
}

// Apply writes a batch in one pipelined round trip. HSETNX makes each write
// idempotent: a reading delivered twice (a resend through a different gateway
// replica, or a redelivery after a rebalance) leaves the first value in place.
// Returns how many were new.
//
// Every LagSampleEvery-th reading also records a latency sample
// "total_ms,log_to_apply_ms,partition,new": total is first send to apply,
// log_to_apply is Kafka timestamp to apply, so the two halves of a slow
// reading (edge side, consumer side) can be told apart.
func (s *Store) Apply(ctx context.Context, batch []Item, now time.Time) (newCount int, err error) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.BoolCmd, len(batch))
	nowMs := now.UnixMilli()
	for i, it := range batch {
		r := it.Reading
		cmds[i] = pipe.HSetNX(ctx, DeviceKey(r.DeviceId), strconv.FormatUint(r.Sequence, 10), EncodeValue(r.Value))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	stats := s.rdb.Pipeline()
	for i, c := range cmds {
		fresh := c.Val()
		if fresh {
			newCount++
		}
		s.n++
		it := batch[i]
		if s.n%LagSampleEvery == 0 && it.Reading.TimestampMs > 0 {
			isNew := 0
			if fresh {
				isNew = 1
			}
			stats.RPush(ctx, "lag", fmt.Sprintf("%d,%d,%d,%d",
				nowMs-it.Reading.TimestampMs, nowMs-it.LogTime.UnixMilli(), it.Partition, isNew))
		}
	}
	stats.IncrBy(ctx, "stats:new", int64(newCount))
	stats.IncrBy(ctx, "stats:dup", int64(len(batch)-newCount))
	stats.IncrBy(ctx, "tl:"+strconv.FormatInt(nowMs/100, 10), int64(len(batch)))
	_, err = stats.Exec(ctx)
	return newCount, err
}

// Fetcher is what Run needs from a consumer-group reader.
type Fetcher interface {
	Fetch(ctx context.Context) (kafka.Message, *pb.Reading, error)
	Commit(ctx context.Context, msgs ...kafka.Message) error
}

// Run consumes until ctx ends: fetch up to maxBatch messages (or whatever
// arrived within linger), apply them, then commit their offsets. Commit after
// apply is what makes the group at-least-once: a member killed between the
// two leaves the batch uncommitted, and the next owner of those partitions
// applies it again, harmlessly.
func Run(ctx context.Context, f Fetcher, store *Store, maxBatch int, linger time.Duration, onBatch func(n, fresh int, partitions map[int]bool)) error {
	for ctx.Err() == nil {
		msgs := make([]kafka.Message, 0, maxBatch)
		items := make([]Item, 0, maxBatch)
		parts := map[int]bool{}
		deadline := time.Now().Add(linger)
		for len(msgs) < maxBatch {
			fctx := ctx
			var cancel context.CancelFunc
			if len(msgs) > 0 {
				fctx, cancel = context.WithDeadline(ctx, deadline)
			}
			m, r, err := f.Fetch(fctx)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				break // linger expired with a partial batch, or a transient fetch error
			}
			msgs = append(msgs, m)
			parts[m.Partition] = true
			if r != nil {
				items = append(items, Item{Reading: r, Partition: m.Partition, LogTime: m.Time})
			}
		}
		if len(msgs) == 0 {
			continue
		}
		fresh, err := store.Apply(ctx, items, time.Now())
		if err != nil {
			return err
		}
		if err := f.Commit(ctx, msgs...); err != nil && ctx.Err() == nil {
			// typically a rebalance took these partitions away mid-batch; the new
			// owner reapplies from the last committed offset, which is harmless
			log.Printf("applier: commit failed (batch will be redelivered): %v", err)
		}
		if onBatch != nil {
			onBatch(len(items), fresh, parts)
		}
	}
	return nil
}
