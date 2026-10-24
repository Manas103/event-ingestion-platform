package supplyrollup

import (
	"crypto/sha256"
	"sort"
)

// Digest reduces a cell-to-sorted-device-ids map to one deterministic
// SHA-256 digest: sort by cell id, then hash each cell id followed by its
// already-sorted member list. Two independently produced snapshots that
// reduce to the same digest are, for this project's purposes,
// "bit-identical", the same construction internal/ingest.FinalStateDigest
// uses for the base gateway's own replay claim.
func Digest(byCell map[string][]string) []byte {
	cells := make([]string, 0, len(byCell))
	for c := range byCell {
		cells = append(cells, c)
	}
	sort.Strings(cells)

	h := sha256.New()
	for _, cell := range cells {
		h.Write([]byte(cell))
		h.Write([]byte{0})
		for _, device := range byCell[cell] {
			h.Write([]byte(device))
			h.Write([]byte{0})
		}
		h.Write([]byte{1})
	}
	return h.Sum(nil)
}
