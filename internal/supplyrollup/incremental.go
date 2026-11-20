// Package supplyrollup maintains, per H3-inspired cell, the set of devices
// currently positioned there, fed by the location-ping rollup consumer as
// pings land from Kafka. IncrementalEngine is the production path;
// recompute.go is an independently written from-scratch answer used only to
// check this one is right, the same split as internal/segment's
// IncrementalEngine and Recompute.
package supplyrollup

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/Manas103/event-ingestion-platform/internal/h3cell"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

func cellKey(cellID string) string { return "supplycell:" + cellID }

// IncrementalEngine tracks each device's current cell with bounded,
// per-device state: a device's position only ever changes cell membership
// in Redis, it never triggers a scan of any other device's state. The
// highest sequence number applied per device is kept in local process
// memory (deviceSeq), not round-tripped through Redis, because the
// benchmark that exercises this engine runs it as a single consumer
// goroutine; that in-process map is what makes "latest sequence wins"
// correct even when the consumer sees a fully reordered and duplicated
// stream, exactly the property the bit-identical claim depends on.
type IncrementalEngine struct {
	rdb *redis.Client

	mu         sync.Mutex
	deviceSeq  map[string]uint64
	deviceCell map[string]string
}

func NewIncrementalEngine(rdb *redis.Client) *IncrementalEngine {
	return &IncrementalEngine{
		rdb:        rdb,
		deviceSeq:  make(map[string]uint64),
		deviceCell: make(map[string]string),
	}
}

// ApplyPing updates one device's cell membership from one LocationPing. A
// ping whose sequence is not strictly newer than the last one already
// applied for that device is ignored: it is either the duplicate dedup
// already let through once, or a reordered delivery of a ping the device's
// position has already moved past.
func (e *IncrementalEngine) ApplyPing(ctx context.Context, ping *pb.LocationPing) error {
	e.mu.Lock()
	if existing, ok := e.deviceSeq[ping.DeviceId]; ok && existing >= ping.Sequence {
		e.mu.Unlock()
		return nil
	}
	cell := h3cell.CellID(ping.Lat, ping.Lon)
	oldCell, hadOld := e.deviceCell[ping.DeviceId]
	e.deviceSeq[ping.DeviceId] = ping.Sequence
	e.deviceCell[ping.DeviceId] = cell
	e.mu.Unlock()

	if hadOld && oldCell == cell {
		return nil
	}

	pipe := e.rdb.Pipeline()
	if hadOld {
		pipe.SRem(ctx, cellKey(oldCell), ping.DeviceId)
	}
	pipe.SAdd(ctx, cellKey(cell), ping.DeviceId)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("apply ping cell membership: %w", err)
	}
	return nil
}

// Snapshot returns, for every cell any device has ever occupied, the sorted
// list of device ids currently assigned to it. Returning full membership
// rather than just a count is what makes the digest comparison in digest.go
// a real "bit-identical" check instead of one that could pass on a
// coincidentally equal count with the wrong devices in it.
func (e *IncrementalEngine) Snapshot(ctx context.Context) (map[string][]string, error) {
	e.mu.Lock()
	cells := make(map[string]bool)
	for _, c := range e.deviceCell {
		cells[c] = true
	}
	e.mu.Unlock()

	out := make(map[string][]string, len(cells))
	for cell := range cells {
		members, err := e.rdb.SMembers(ctx, cellKey(cell)).Result()
		if err != nil {
			return nil, fmt.Errorf("smembers %s: %w", cell, err)
		}
		if len(members) == 0 {
			continue
		}
		sort.Strings(members)
		out[cell] = members
	}
	return out, nil
}
