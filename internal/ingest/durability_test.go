package ingest

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// A gateway killed while a drain is retrying must still find every spooled
// record on disk when it restarts (the old Drain truncated first).
func TestDrainWithKeepsRecordsOnDiskUntilRetryFinishes(t *testing.T) {
	path := t.TempDir() + "/spool.dat"
	s, err := NewSpool(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 50; i++ {
		if err := s.Write(&pb.Reading{DeviceId: "d", Sequence: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	var onDiskDuringRetry int
	_, _, err = s.DrainWith(func(rs []*pb.Reading) []*pb.Reading {
		// what a restarted process would read if this one died right now
		s2, err := NewSpool(path)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := readRecordsFromPath(t, s2)
		onDiskDuringRetry = len(got)
		s2.Close()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if onDiskDuringRetry != 50 {
		t.Fatalf("records on disk during retry = %d, want 50", onDiskDuringRetry)
	}
	if left, _ := s.SizeBytes(); left != 0 {
		t.Fatalf("spool not emptied after a fully successful drain: %d bytes", left)
	}
	s.Close()
}

// Records that still fail go back to the file, and records written while the
// retry was running are kept too.
func TestDrainWithKeepsFailuresAndConcurrentWrites(t *testing.T) {
	s := newTempSpool(t)
	for i := 1; i <= 10; i++ {
		s.Write(&pb.Reading{DeviceId: "d", Sequence: uint64(i)})
	}
	drained, respooled, err := s.DrainWith(func(rs []*pb.Reading) []*pb.Reading {
		s.Write(&pb.Reading{DeviceId: "d", Sequence: 100}) // arrives mid-drain
		return rs[:3]
	})
	if err != nil || drained != 7 || respooled != 3 {
		t.Fatalf("drained=%d respooled=%d err=%v, want 7, 3, nil", drained, respooled, err)
	}
	left, err := s.Drain()
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	for _, r := range left {
		seqs = append(seqs, r.Sequence)
	}
	if len(seqs) != 4 || seqs[3] != 100 {
		t.Fatalf("left in spool = %v, want the 3 failures then 100", seqs)
	}
}

type failingProducer struct{}

func (failingProducer) Produce(context.Context, *pb.Reading) error { return errors.New("down") }

// If a reading can be neither produced nor spooled it is not durable, so it
// must not be acked and its dedup entry must be rolled back: otherwise the
// device's resend would be dropped as a duplicate of a reading nobody kept.
func TestUndurableReadingIsForgottenSoItsResendIsAdmitted(t *testing.T) {
	s := newTempSpool(t)
	s.Close() // every spool write now fails
	p := NewPipeline(NewDedup(), s, failingProducer{})
	r := &pb.Reading{DeviceId: "d", Sequence: 7}
	if err := p.Ingest(context.Background(), r); err == nil {
		t.Fatal("Ingest returned nil for a reading nothing kept")
	}
	if !p.Dedup().Admit("d", 7) {
		t.Fatal("resend of the undurable reading was treated as a duplicate")
	}
}

func readRecordsFromPath(t *testing.T, s *Spool) ([]*pb.Reading, error) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	return readRecords(io.NewSectionReader(s.file, 0, info.Size()))
}
