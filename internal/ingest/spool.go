package ingest

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// Spool is an append-only, length-prefixed file of protobuf Readings. It is
// where a Reading goes when the downstream Kafka path cannot take it right
// now (a full producer, or the broker being unreachable), so backpressure
// degrades into "written to disk, forward it later" instead of "dropped".
type Spool struct {
	mu      sync.Mutex
	drainMu sync.Mutex // one drain at a time (ticker, admin RPC, shutdown)
	path    string
	file    *os.File
	w       *bufio.Writer
	sync    bool
}

func NewSpool(path string) (*Spool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return &Spool{path: path, file: f, w: bufio.NewWriter(f)}, nil
}

// SetSync makes every Write fsync before returning. Without it a written
// record survives the gateway process being killed (it is in the kernel page
// cache) but not the node losing power; with it, a record the edge has acked
// survives both, at the cost of one fsync per spooled record.
func (s *Spool) SetSync(on bool) {
	s.mu.Lock()
	s.sync = on
	s.mu.Unlock()
}

func (s *Spool) Write(r *pb.Reading) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := proto.Marshal(r)
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
	if err := s.w.Flush(); err != nil {
		return err
	}
	if s.sync {
		return s.file.Sync()
	}
	return nil
}

// Drain reads every record currently in the spool file, truncates it, and
// returns the records for the caller to retry. The lock is held for the
// whole read-then-truncate so a concurrent Write can never land between the
// read and the truncate and be silently discarded.
func (s *Spool) Drain() ([]*pb.Reading, error) {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.w.Flush(); err != nil {
		return nil, err
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	r := bufio.NewReader(s.file)
	var out []*pb.Reading
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			break // EOF or a short trailing write: end of usable spool content
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			break
		}
		var reading pb.Reading
		if err := proto.Unmarshal(buf, &reading); err != nil {
			continue // corrupt record: skip rather than abort the whole drain
		}
		out = append(out, &reading)
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

// DrainWith is the crash-safe drain the pipeline uses. Drain above truncates
// the file before the caller has retried anything, so a gateway killed between
// the two loses every spooled record, including ones it had already acked to a
// device. DrainWith instead leaves the file untouched while retry runs, then
// atomically replaces it with the records retry hands back (still failing)
// plus anything Written meanwhile. A crash at any point leaves every record on
// disk at least once; the cost is that a record produced just before a crash
// is produced again after restart, which the downstream dedup absorbs.
func (s *Spool) DrainWith(retry func([]*pb.Reading) []*pb.Reading) (drained, respooled int, err error) {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()

	s.mu.Lock()
	if err := s.w.Flush(); err != nil {
		s.mu.Unlock()
		return 0, 0, err
	}
	info, err := s.file.Stat()
	if err != nil {
		s.mu.Unlock()
		return 0, 0, err
	}
	end := info.Size()
	records, err := readRecords(io.NewSectionReader(s.file, 0, end))
	s.mu.Unlock()
	if err != nil || len(records) == 0 {
		return 0, 0, err
	}

	failed := retry(records)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return 0, 0, err
	}
	info, err = s.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	tail := make([]byte, info.Size()-end)
	if _, err := s.file.ReadAt(tail, end); err != nil && err != io.EOF {
		return 0, 0, err
	}

	tmpPath := s.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return 0, 0, err
	}
	tw := bufio.NewWriter(tmp)
	for _, r := range failed {
		if err := writeRecord(tw, r); err != nil {
			tmp.Close()
			return 0, 0, err
		}
	}
	if _, err := tw.Write(tail); err != nil {
		tmp.Close()
		return 0, 0, err
	}
	if err := tw.Flush(); err != nil {
		tmp.Close()
		return 0, 0, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return 0, 0, err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		tmp.Close()
		return 0, 0, err
	}
	s.file.Close()
	if _, err := tmp.Seek(0, io.SeekEnd); err != nil {
		return 0, 0, err
	}
	s.file = tmp
	s.w = bufio.NewWriter(tmp)
	return len(records) - len(failed), len(failed), nil
}

func writeRecord(w io.Writer, r *pb.Reading) error {
	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readRecords(src io.Reader) ([]*pb.Reading, error) {
	r := bufio.NewReader(src)
	var out []*pb.Reading
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return out, nil // EOF or a short trailing write
		}
		buf := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return out, nil
		}
		var reading pb.Reading
		if err := proto.Unmarshal(buf, &reading); err != nil {
			continue
		}
		out = append(out, &reading)
	}
}

// SizeBytes reports how much unflushed content currently sits in the spool
// file, without draining it. Used to poll "has the backlog been cleared yet"
// after an outage, rather than guessing a fixed wait: a large backlog
// produced during a long outage takes a while to drain one record at a time,
// and that time scales with the backlog, not with the outage duration.
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
