package kafkaclient

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/Manas103/event-ingestion-platform/internal/h3cell"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// LocationPingProducer publishes each LocationPing keyed by its H3-inspired
// cell id rather than by device id: the per-cell supply rollup consumer
// needs every ping for one cell on the same partition to see a stable
// relative order, the same reason the base gateway keys Reading by device
// id for its own per-device ordering guarantee.
type LocationPingProducer struct {
	writer *kafka.Writer
}

func NewLocationPingProducer(brokerAddr, topic string) *LocationPingProducer {
	return &LocationPingProducer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 2 * time.Second,
		RequiredAcks: kafka.RequireOne,
	}}
}

func (p *LocationPingProducer) Produce(ctx context.Context, ping *pb.LocationPing) error {
	value, err := proto.Marshal(ping)
	if err != nil {
		return err
	}
	cell := h3cell.CellID(ping.Lat, ping.Lon)
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(cell),
		Value: value,
	})
}

func (p *LocationPingProducer) Close() error {
	return p.writer.Close()
}

// ConsumeAvailableLocationPings reads every message currently on every
// partition of topic, from the earliest offset up to each partition's high
// watermark at call time, by dialing each partition's leader directly
// rather than using a consumer group. See ConsumeAvailable's doc comment for
// why: a rebalance round trip's latency has no place in a "did everything I
// sent arrive" verification pass.
func ConsumeAvailableLocationPings(ctx context.Context, brokerAddr, topic string) ([]*pb.LocationPing, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	var out []*pb.LocationPing
	for _, part := range partitions {
		pings, err := readLocationPingPartition(ctx, brokerAddr, topic, part.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", part.ID, err)
		}
		out = append(out, pings...)
	}
	return out, nil
}

func readLocationPingPartition(ctx context.Context, brokerAddr, topic string, partition int) ([]*pb.LocationPing, error) {
	pconn, err := kafka.DialLeader(ctx, "tcp", brokerAddr, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("dial leader: %w", err)
	}
	defer pconn.Close()

	first, last, err := pconn.ReadOffsets()
	if err != nil {
		return nil, fmt.Errorf("read offsets: %w", err)
	}
	if last <= first {
		return nil, nil
	}

	if _, err := pconn.Seek(first, kafka.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	var out []*pb.LocationPing
	batch := pconn.ReadBatch(1, 50<<20)
	defer batch.Close()

	buf := make([]byte, 1<<20)
	for {
		n, err := batch.Read(buf)
		if n > 0 {
			var ping pb.LocationPing
			if unmarshalErr := proto.Unmarshal(buf[:n], &ping); unmarshalErr == nil {
				out = append(out, &ping)
			}
		}
		if err != nil {
			break
		}
		if int64(len(out)) >= last-first {
			break
		}
	}
	return out, nil
}
