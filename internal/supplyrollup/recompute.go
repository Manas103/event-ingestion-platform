package supplyrollup

import (
	"sort"

	"github.com/Manas103/event-ingestion-platform/internal/h3cell"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// Recompute answers "which devices are in which cell" from zero: given the
// full set of accepted (already deduped) pings in any order, take each
// device's highest-sequence ping as its true final position and group by
// cell. It shares no code with IncrementalEngine, so a bug common to both
// could not be present here by construction.
func Recompute(pings []*pb.LocationPing) map[string][]string {
	finalSeq := make(map[string]uint64)
	finalCell := make(map[string]string)
	for _, p := range pings {
		if existing, ok := finalSeq[p.DeviceId]; ok && existing >= p.Sequence {
			continue
		}
		finalSeq[p.DeviceId] = p.Sequence
		finalCell[p.DeviceId] = h3cell.CellID(p.Lat, p.Lon)
	}

	byCell := make(map[string][]string)
	for device, cell := range finalCell {
		byCell[cell] = append(byCell[cell], device)
	}
	for cell := range byCell {
		sort.Strings(byCell[cell])
	}
	return byCell
}
