// applier is one member of the readings consumer group: it applies every
// reading on its assigned partitions to Redis idempotently and commits
// offsets after applying (see internal/applier). Run N replicas for N-way
// partition parallelism (up to the partition count).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Manas103/event-ingestion-platform/internal/applier"
	"github.com/Manas103/event-ingestion-platform/internal/kafkaclient"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "comma-separated Kafka bootstrap brokers")
	topic := flag.String("topic", "telemetry", "kafka topic")
	group := flag.String("group", "readings-applier", "consumer group id")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	maxBatch := flag.Int("batch", 500, "max readings per apply")
	linger := flag.Duration("linger", 20*time.Millisecond, "max wait to fill a batch")
	member := flag.String("member", os.Getenv("POD_NAME"), "member name for logs")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()
	brokerList := kafkaclient.SplitBrokers(*brokers)
	if err := kafkaclient.WaitForTopic(ctx, brokerList, *topic); err != nil {
		log.Fatalf("applier %s: waiting for topic %q: %v", *member, *topic, err)
	}
	reader := kafkaclient.NewGroupReader(brokerList, *topic, *group)
	defer reader.Close()

	var (
		mu      sync.Mutex
		applied int
		fresh   int
		owned   = map[int]bool{}
	)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				mu.Lock()
				ps := make([]int, 0, len(owned))
				for p := range owned {
					ps = append(ps, p)
				}
				sort.Ints(ps)
				log.Printf("applier %s: last 5s applied=%d new=%d partitions=%v", *member, applied, fresh, ps)
				applied, fresh, owned = 0, 0, map[int]bool{}
				mu.Unlock()
			}
		}
	}()

	log.Printf("applier %s: group %q topic %q redis %s", *member, *group, *topic, *redisAddr)
	err := applier.Run(ctx, reader, applier.NewStore(rdb), *maxBatch, *linger, func(n, f int, parts map[int]bool) {
		mu.Lock()
		applied += n
		fresh += f
		for p := range parts {
			owned[p] = true
		}
		mu.Unlock()
	})
	if err != nil {
		log.Fatalf("applier %s: %v", *member, err)
	}
	log.Printf("applier %s: stopped", *member)
}
