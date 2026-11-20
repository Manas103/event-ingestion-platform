// Package simfleet builds and drives simulated location-reporting devices
// over real WebSocket connections, the LocationPing analogue of
// internal/simdevice. Each device moves through a short path of cells
// across its own sequence of pings, rather than staying in one cell for its
// whole stream, so a replay that gets delivery order wrong would actually
// change which cell the device is credited to.
package simfleet

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// DeviceStream is one simulated device's delivery order: what it will
// actually send, over its own WebSocket connection, in send order.
type DeviceStream struct {
	DeviceID string
	Delivery []*pb.LocationPing
}

// AllDelivered returns every ping across every stream, duplicates and all,
// in no particular cross-device order. Recompute's highest-sequence-wins
// rule makes it safe to pass this directly as the reference oracle's input
// without a separate dedup pass.
func AllDelivered(streams []DeviceStream) []*pb.LocationPing {
	var out []*pb.LocationPing
	for _, s := range streams {
		out = append(out, s.Delivery...)
	}
	return out
}

// BuildReplayCorpus builds numDevices independent devices, each moving
// through pingsPerDevice sequential positions that step around a small ring
// of cells (so a device's final cell genuinely depends on which of its
// readings is actually its highest sequence number), then perturbs each
// device's own delivery order so that exactly dupFraction of the full
// delivered stream is duplicate traffic, and shuffles every device's
// delivery into a non-monotonic send order.
func BuildReplayCorpus(numDevices, pingsPerDevice int, dupFraction float64, seed int64) []DeviceStream {
	rng := rand.New(rand.NewSource(seed))
	streams := make([]DeviceStream, numDevices)
	now := time.Now().UnixMilli()

	dupCount := int(math.Round(dupFraction * float64(pingsPerDevice) / (1 - dupFraction)))

	for d := 0; d < numDevices; d++ {
		deviceID := fmt.Sprintf("dev-%05d", d)
		baseLat := 40.70 + 0.01*float64(d%20)
		baseLon := -74.02 + 0.01*float64((d/20)%20)

		unique := make([]*pb.LocationPing, pingsPerDevice)
		for i := 0; i < pingsPerDevice; i++ {
			// Step through a small ring of 4 neighboring cells as the
			// sequence advances, so the device's true final cell is
			// whichever step carries the highest sequence number, not
			// whichever one happens to be delivered last.
			step := i % 4
			unique[i] = &pb.LocationPing{
				DeviceId:    deviceID,
				Sequence:    uint64(i + 1),
				TimestampMs: now,
				Lat:         baseLat + 0.0045*float64(step/2),
				Lon:         baseLon + 0.0045*float64(step%2),
			}
		}

		delivery := make([]*pb.LocationPing, 0, pingsPerDevice+dupCount)
		delivery = append(delivery, unique...)
		for i := 0; i < dupCount; i++ {
			delivery = append(delivery, unique[rng.Intn(pingsPerDevice)])
		}
		rng.Shuffle(len(delivery), func(i, j int) { delivery[i], delivery[j] = delivery[j], delivery[i] })

		streams[d] = DeviceStream{DeviceID: deviceID, Delivery: delivery}
	}
	return streams
}

// Send opens one WebSocket connection per stream (bounded to `concurrency`
// connections open at a time) and writes every LocationPing in that
// device's delivery order as a single binary protobuf frame.
func Send(ctx context.Context, wsURL string, streams []DeviceStream, concurrency int) error {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, len(streams))

	for _, stream := range streams {
		wg.Add(1)
		sem <- struct{}{}
		go func(s DeviceStream) {
			defer wg.Done()
			defer func() { <-sem }()

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
			if err != nil {
				errCh <- fmt.Errorf("device %s: dial: %w", s.DeviceID, err)
				return
			}
			defer conn.Close()

			for _, p := range s.Delivery {
				b, err := proto.Marshal(p)
				if err != nil {
					errCh <- fmt.Errorf("device %s: marshal: %w", s.DeviceID, err)
					return
				}
				if err := conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
					errCh <- fmt.Errorf("device %s: write: %w", s.DeviceID, err)
					return
				}
			}
		}(stream)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
