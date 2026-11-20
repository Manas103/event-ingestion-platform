package supplyrollup

import (
	"bytes"
	"math/rand"
	"testing"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// TestRecomputeIgnoresStaleSequence pins the "latest sequence wins" rule: a
// device's earlier, lower-sequence position must not override its later
// one regardless of delivery order.
func TestRecomputeIgnoresStaleSequence(t *testing.T) {
	pings := []*pb.LocationPing{
		{DeviceId: "d1", Sequence: 1, Lat: 40.70, Lon: -74.00},
		{DeviceId: "d1", Sequence: 2, Lat: 40.80, Lon: -73.90},
	}
	before := Recompute(pings)

	reversed := []*pb.LocationPing{pings[1], pings[0]}
	after := Recompute(reversed)

	if !bytes.Equal(Digest(before), Digest(after)) {
		t.Fatalf("expected delivery order not to change the recomputed cell assignment")
	}
}

// TestDigestStableUnderReorderAndDuplication proves Digest depends only on
// the final per-cell membership, not on how many times or in what order
// the pings that produced it arrived, the exact property the replay check
// ("bit-identical after a fully reordered replay") rests on.
func TestDigestStableUnderReorderAndDuplication(t *testing.T) {
	var canonical []*pb.LocationPing
	for d := 0; d < 20; d++ {
		for s := uint64(1); s <= 5; s++ {
			canonical = append(canonical, &pb.LocationPing{
				DeviceId: deviceName(d),
				Sequence: s,
				Lat:      40.70 + 0.01*float64(d%4),
				Lon:      -74.00 + 0.01*float64(d%3),
			})
		}
	}
	want := Digest(Recompute(canonical))

	rng := rand.New(rand.NewSource(7))
	shuffled := make([]*pb.LocationPing, len(canonical))
	copy(shuffled, canonical)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	duplicated := append(shuffled, canonical[3], canonical[17], canonical[42])

	got := Digest(Recompute(duplicated))
	if !bytes.Equal(want, got) {
		t.Fatalf("digest changed after reorder and duplication: want %x, got %x", want, got)
	}
}

func deviceName(d int) string {
	return "dev-" + string(rune('A'+d))
}
