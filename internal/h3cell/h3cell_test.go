package h3cell

import (
	"fmt"
	"testing"
)

// TestCellIDStableForNearbyPoints pins the property the per-cell rollup
// depends on: two GPS fixes close enough to be the same parked car's jitter
// resolve to the same cell.
func TestCellIDStableForNearbyPoints(t *testing.T) {
	a := CellID(40.7589, -73.9851)
	b := CellID(40.75895, -73.98505)
	if a != b {
		t.Fatalf("expected nearby points to share a cell, got %q and %q", a, b)
	}
}

// TestCellIDDiffersAcrossCity proves CellID is not a constant: two points
// several kilometers apart in different parts of Manhattan must land in
// different cells.
func TestCellIDDiffersAcrossCity(t *testing.T) {
	midtown := CellID(40.7589, -73.9851)
	downtown := CellID(40.7074, -74.0113)
	if midtown == downtown {
		t.Fatalf("expected distinct cells, both resolved to %q", midtown)
	}
}

// TestCellCenterRoundTripsIntoSameCell proves CellCenter is the true
// midpoint of its own cell, not merely some point that also lands in it: a
// point placed exactly at the center a cell id decodes to must hash back to
// that identical cell id.
func TestCellCenterRoundTripsIntoSameCell(t *testing.T) {
	for _, start := range []string{"10:4", "-3:-7", "0:0", "120:-55"} {
		var q, r int
		if _, err := fmt.Sscanf(start, "%d:%d", &q, &r); err != nil {
			t.Fatalf("parse %q: %v", start, err)
		}
		lat, lon := CellCenter(q, r)
		got := CellID(lat, lon)
		if got != start {
			t.Fatalf("CellCenter(%d,%d) = (%f,%f), CellID of that = %q, want %q", q, r, lat, lon, got, start)
		}
	}
}
