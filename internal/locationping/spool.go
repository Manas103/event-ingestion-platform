// Package locationping is the location-ping ingest edge: the same
// dedup-then-produce-or-spool shape as internal/ingest, reusing
// ingest.Dedup directly because the dedup key is the identical
// (device_id, sequence) pair, keyed out to Kafka by H3 cell instead of by
// device id.
package locationping

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// Spool is an append-only, length-prefixed file of protobuf LocationPings,
// the same backpressure design as ingest.Spool: a ping that cannot be
// produced to Kafka right now is written here instead of dropped.
type Spool struct {
	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
}

func NewSpool(path string) (*Spool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return &Spool{file: f, w: bufio.NewWriter(f)}, nil
}

func (s *Spool) Write(p *pb.LocationPing) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := proto.Marshal(p)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := s.w.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	return s.w.Flush()
}

// Drain reads every record currently in the spool file, truncates it, and
// returns the records for the caller to retry, the same read-then-truncate
// locking ingest.Spool uses so a concurrent Write can never land between the
// read and the truncate and be silently discarded.
func (s *Spool) Drain() ([]*pb.LocationPing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.w.Flush(); err != nil {
		return nil, err
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	r := bufio.NewReader(s.file)
	var out []*pb.LocationPing
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			break
		}
		var ping pb.LocationPing
		if err := proto.Unmarshal(buf, &ping); err != nil {
			continue
		}
		out = append(out, &ping)
	}

	if err := s.file.Truncate(0); err != nil {
		return nil, err
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	s.w = bufio.NewWriter(s.file)

	return out, nil
}

func (s *Spool) SizeBytes() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return 0, err
	}
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
