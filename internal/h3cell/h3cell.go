// Package h3cell is an H3-inspired hex grid, not Uber's own H3 library: no
// CGO dependency on libh3 exists on this build machine, so this package
// reimplements the one property this project's claims actually need (every
// lat/lon maps deterministically to one cell id, and nearby points map to
// the same or a neighboring cell) using an axial hex grid over an
// equirectangular projection, not H3's real icosahedral projection. Cell ids
// are this package's own strings, not H3 indexes, and are not
// interchangeable with real H3 cell ids.
package h3cell

import (
	"fmt"
	"math"
)

// cellSpanDegrees is the resolution: the approximate width, in degrees of
// longitude at the reference latitude, of one hex cell. Chosen so that New
// York City's roughly 0.5-degree-wide taxi-trip bounding box resolves into
// several hundred cells, close enough to H3's real resolution 8 (about
// 0.46km edge length) to be a fair stand-in for "index the city in hexes".
const cellSpanDegrees = 0.0045

// refLat is the latitude the longitude span is flattened against, so a
// degree of longitude and a degree of latitude cover comparable ground
// distance across the project's whole operating area (New York City, not a
// global index).
const refLat = 40.75

// CellID buckets (lat, lon) into an axial hex coordinate and returns it as a
// stable string key. Two points within roughly half a cell width of each
// other, accounting for the offset rows of a hex grid, map to the same
// cell.
func CellID(lat, lon float64) string {
	q, r := toAxial(lat, lon)
	return fmt.Sprintf("%d:%d", q, r)
}

// toAxial converts a geographic point to axial hex coordinates (q, r) on a
// flat-top hex grid, following the standard offset-row construction: odd
// rows are shifted half a cell to interlock with their neighbors, which is
// what makes the grid hexagonal rather than a plain rectangle.
func toAxial(lat, lon float64) (int, int) {
	lonSpan := cellSpanDegrees
	latSpan := cellSpanDegrees * math.Cos(refLat*math.Pi/180)

	row := int(math.Floor(lat / latSpan))
	rowOffset := 0.0
	if row%2 != 0 {
		rowOffset = lonSpan / 2
	}
	col := int(math.Floor((lon - rowOffset) / lonSpan))
	return col, row
}

// CellCenter returns the approximate lat/lon center of a cell id, used by
// the simulated fleet to place a driver at a specific cell's midpoint
// rather than an arbitrary point that happens to hash into it.
func CellCenter(q, r int) (lat, lon float64) {
	lonSpan := cellSpanDegrees
	latSpan := cellSpanDegrees * math.Cos(refLat*math.Pi/180)

	lat = (float64(r) + 0.5) * latSpan
	rowOffset := 0.0
	if r%2 != 0 {
		rowOffset = lonSpan / 2
	}
	lon = (float64(q)+0.5)*lonSpan + rowOffset
	return lat, lon
}
