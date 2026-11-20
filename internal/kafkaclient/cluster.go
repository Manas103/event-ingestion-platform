package kafkaclient

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// ProducerConfig is the multi-broker producer the platform gateway uses.
// The original NewProducer (one broker, acks=1) stays as it was for the
// benchmarks measured with it.
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	AcksAll      bool // wait for every in-sync replica, not just the leader
	WriteTimeout time.Duration
}

// NewClusterProducer builds a Producer from cfg. With AcksAll and a topic
// whose min.insync.replicas is 2, a produce that returns nil survives the loss
// of any one broker.
func NewClusterProducer(cfg ProducerConfig) *Producer {
	acks := kafka.RequireOne
	if cfg.AcksAll {
		acks = kafka.RequireAll
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 2 * time.Second
	}
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: acks,
		MaxAttempts:  1, // the pipeline spools on failure; retrying here would only hold the caller
		// pick up a partition's new leader within 1 s of a broker failover
		// instead of kafka-go's 6 s default. Measured: it did not change
		// broker-crash recovery, which is set by Kafka's own controller
		// election and leader moves (DESIGN.md, broker crash), but it bounds
		// the client's share of it.
		Transport: &kafka.Transport{MetadataTTL: time.Second},
	}}
}

// SplitBrokers parses a comma-separated broker list.
func SplitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// EnsureReplicatedTopic creates topic with the given partitions, replication
// factor and min.insync.replicas if it does not exist yet. It tries each
// broker in turn, so it works while one of them is down.
func EnsureReplicatedTopic(brokers []string, topic string, partitions, replication, minISR int) error {
	var lastErr error
	for _, b := range brokers {
		conn, err := kafka.Dial("tcp", b)
		if err != nil {
			lastErr = err
			continue
		}
		controller, err := conn.Controller()
		conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		ctrl, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			lastErr = err
			continue
		}
		err = ctrl.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replication,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "min.insync.replicas", ConfigValue: strconv.Itoa(minISR)},
			},
		})
		ctrl.Close()
		if err == nil || strings.Contains(err.Error(), "already exists") {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("create topic %q: %w", topic, lastErr)
}

// GroupReader is a consumer-group member over the readings topic. Offsets are
// committed by the caller after the readings are applied, so a member that
// dies mid-batch leaves its uncommitted readings to be redelivered to
// whichever member takes over the partition (at least once).
type GroupReader struct {
	r *kafka.Reader
}

func NewGroupReader(brokers []string, topic, group string) *GroupReader {
	return &GroupReader{r: kafka.NewReader(kafka.ReaderConfig{
		Brokers:           brokers,
		Topic:             topic,
		GroupID:           group,
		MinBytes:          1,
		MaxBytes:          8 << 20,
		MaxWait:           100 * time.Millisecond,
		HeartbeatInterval: time.Second,
		SessionTimeout:    6 * time.Second,
		RebalanceTimeout:  6 * time.Second,
		StartOffset:       kafka.FirstOffset,
		CommitInterval:    0, // synchronous commits, issued by the caller after applying
		// rejoin 0.5 s after losing the group coordinator instead of kafka-go's
		// 5 s default. The measured ~13 s consumer stall after a broker crash
		// is the coordinator move plus a full rebalance, and was the same with
		// the default (docs/platform/client-defaults).
		JoinGroupBackoff: 500 * time.Millisecond,
		// without this a member that joined before the topic had partitions
		// is assigned nothing and never looks again
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 2 * time.Second,
	})}
}

// WaitForTopic blocks until topic exists with at least one partition, or ctx
// ends. A consumer-group member that joins before the topic exists gets an
// empty assignment, so the applier waits for the gateway to create it.
func WaitForTopic(ctx context.Context, brokers []string, topic string) error {
	for {
		for _, b := range brokers {
			conn, err := kafka.DialContext(ctx, "tcp", b)
			if err != nil {
				continue
			}
			parts, err := conn.ReadPartitions(topic)
			conn.Close()
			if err == nil && len(parts) > 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Fetch returns the next message and its decoded Reading.
func (g *GroupReader) Fetch(ctx context.Context) (kafka.Message, *pb.Reading, error) {
	m, err := g.r.FetchMessage(ctx)
	if err != nil {
		return m, nil, err
	}
	var r pb.Reading
	if err := proto.Unmarshal(m.Value, &r); err != nil {
		return m, nil, err
	}
	return m, &r, nil
}

func (g *GroupReader) Commit(ctx context.Context, msgs ...kafka.Message) error {
	return g.r.CommitMessages(ctx, msgs...)
}

func (g *GroupReader) Close() error { return g.r.Close() }

// ConsumeAllFrom reads every message on every partition of topic from the
// earliest offset to the high watermark, from whichever broker currently
// leads each partition. Used by the verifier to count what actually landed in
// the log, duplicates included.
func ConsumeAllFrom(ctx context.Context, brokers []string, topic string) ([]*pb.Reading, error) {
	var lastErr error
	for _, b := range brokers {
		out, err := consumeAllVia(ctx, b, topic)
		if err == nil {
			return out, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func consumeAllVia(ctx context.Context, broker, topic string) ([]*pb.Reading, error) {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, err
	}
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		return nil, err
	}
	var out []*pb.Reading
	for _, p := range parts {
		leader := net.JoinHostPort(p.Leader.Host, strconv.Itoa(p.Leader.Port))
		got, err := readPartitionAll(ctx, leader, topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d via %s: %w", p.ID, leader, err)
		}
		out = append(out, got...)
	}
	return out, nil
}

// readPartitionAll reads [first, last) of one partition in as many fetches as
// it takes (readPartition above assumes one fetch returns everything).
func readPartitionAll(ctx context.Context, leader, topic string, partition int) ([]*pb.Reading, error) {
	pconn, err := kafka.DialLeader(ctx, "tcp", leader, topic, partition)
	if err != nil {
		return nil, err
	}
	defer pconn.Close()
	first, last, err := pconn.ReadOffsets()
	if err != nil {
		return nil, err
	}
	var out []*pb.Reading
	next := first
	for next < last {
		before := next
		if _, err := pconn.Seek(next, kafka.SeekAbsolute); err != nil {
			return nil, err
		}
		pconn.SetReadDeadline(time.Now().Add(10 * time.Second))
		batch := pconn.ReadBatch(1, 16<<20)
		for next < last {
			m, err := batch.ReadMessage()
			if err != nil {
				break
			}
			next = m.Offset + 1
			var r pb.Reading
			if err := proto.Unmarshal(m.Value, &r); err == nil {
				out = append(out, &r)
			}
		}
		if err := batch.Close(); err != nil && next < last {
			return nil, fmt.Errorf("offset %d of %d: %w", next, last, err)
		}
		if next == before {
			return nil, fmt.Errorf("no progress at offset %d of %d", next, last)
		}
	}
	return out, nil
}
