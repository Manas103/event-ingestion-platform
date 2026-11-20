package simdevice

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	pb "github.com/Manas103/event-ingestion-platform/proto"
)

// AckedConfig drives SendAcked: each device keeps up to Window readings sent
// but not yet acked, sends fresh readings at Rate per second, and on any
// connection failure reconnects (to the next URL, round robin) and resends
// every unacked reading before continuing.
type AckedConfig struct {
	URLs        []string      // ws://host/ingest?ack=1 endpoints; a Kubernetes Service is one URL
	Rate        float64       // fresh readings per second per device; 0 sends as fast as the window allows
	Window      int           // max unacked readings per device
	Bucket      time.Duration // timeline resolution
	MaxDuration time.Duration // timeline length (runs longer than this still finish, untracked)
}

// AckedStats is what one SendAcked run measured.
type AckedStats struct {
	T0            time.Time
	Elapsed       time.Duration
	Sent          uint64 // fresh frames, duplicates in the delivery order included
	Resent        uint64 // frames resent after a reconnect
	Acked         uint64
	Disconnects   uint64
	DialFailures  uint64
	ReconnectGaps []time.Duration // per disconnect: connection lost until the first ack after reconnecting
	AckTimeline   []uint64        // acks per Bucket since T0
	DropTimeline  []uint64        // disconnects per Bucket since T0
}

type timeline struct {
	t0     time.Time
	bucket time.Duration
	acks   []atomic.Uint64
	drops  []atomic.Uint64
}

func (t *timeline) idx(now time.Time) int {
	i := int(now.Sub(t.t0) / t.bucket)
	if i < 0 || i >= len(t.acks) {
		return -1
	}
	return i
}

// SendAcked runs every stream to completion: it returns once every reading
// of every device has been acked at least once, or ctx ends.
func SendAcked(ctx context.Context, streams []DeviceStream, cfg AckedConfig) (AckedStats, error) {
	if cfg.Window <= 0 {
		cfg.Window = 64
	}
	if cfg.Bucket <= 0 {
		cfg.Bucket = 100 * time.Millisecond
	}
	if cfg.MaxDuration <= 0 {
		cfg.MaxDuration = 10 * time.Minute
	}
	n := int(cfg.MaxDuration / cfg.Bucket)
	tl := &timeline{t0: time.Now(), bucket: cfg.Bucket, acks: make([]atomic.Uint64, n), drops: make([]atomic.Uint64, n)}

	var (
		stats    AckedStats
		gapsMu   sync.Mutex
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	stats.T0 = tl.t0
	for i, s := range streams {
		wg.Add(1)
		go func(i int, s DeviceStream) {
			defer wg.Done()
			d := &device{stream: s, cfg: cfg, tl: tl, urlIdx: i % len(cfg.URLs), rng: rand.New(rand.NewSource(int64(i)))}
			if err := d.run(ctx); err != nil {
				errOnce.Do(func() { firstErr = err })
			}
			atomic.AddUint64(&stats.Sent, d.sent)
			atomic.AddUint64(&stats.Resent, d.resent)
			atomic.AddUint64(&stats.Acked, d.acked)
			atomic.AddUint64(&stats.Disconnects, d.disconnects)
			atomic.AddUint64(&stats.DialFailures, d.dialFailures)
			gapsMu.Lock()
			stats.ReconnectGaps = append(stats.ReconnectGaps, d.gaps...)
			gapsMu.Unlock()
		}(i, s)
	}
	wg.Wait()
	stats.Elapsed = time.Since(tl.t0)
	last := tl.idx(time.Now())
	if last < 0 {
		last = n - 1
	}
	for i := 0; i <= last; i++ {
		stats.AckTimeline = append(stats.AckTimeline, tl.acks[i].Load())
		stats.DropTimeline = append(stats.DropTimeline, tl.drops[i].Load())
	}
	sort.Slice(stats.ReconnectGaps, func(a, b int) bool { return stats.ReconnectGaps[a] < stats.ReconnectGaps[b] })
	return stats, firstErr
}

type device struct {
	stream DeviceStream
	cfg    AckedConfig
	tl     *timeline
	urlIdx int
	rng    *rand.Rand

	next         int            // next fresh reading in stream.Delivery
	unacked      map[uint64]int // sequence -> frames outstanding
	bySeq        map[uint64]*pb.Reading
	lostAt       time.Time // set while a disconnect has not been followed by an ack
	gaps         []time.Duration
	sent         uint64
	resent       uint64
	acked        uint64
	disconnects  uint64
	dialFailures uint64
}

func (d *device) outstanding() int {
	n := 0
	for _, c := range d.unacked {
		n += c
	}
	return n
}

func (d *device) done() bool { return d.next >= len(d.stream.Delivery) && len(d.unacked) == 0 }

func (d *device) run(ctx context.Context) error {
	d.unacked = make(map[uint64]int)
	d.bySeq = make(map[uint64]*pb.Reading)
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	backoff := 100 * time.Millisecond
	for !d.done() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		url := d.cfg.URLs[d.urlIdx%len(d.cfg.URLs)]
		conn, _, err := dialer.DialContext(ctx, url, nil)
		if err != nil {
			d.dialFailures++
			d.urlIdx++
			// jittered exponential backoff, capped, so a whole fleet does not redial in lockstep
			sleep := backoff/2 + time.Duration(d.rng.Int63n(int64(backoff)))
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return ctx.Err()
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 100 * time.Millisecond
		if err := d.session(ctx, conn); err != nil {
			return err
		}
		// a connection that dropped most likely means its replica died: try
		// the next one first instead of spending a failed dial on the same one
		// (with a single load-balanced URL this changes nothing)
		if !d.done() {
			d.urlIdx++
		}
	}
	return nil
}

// session runs one connection until the stream is fully acked (returns nil
// with d.done()) or the connection fails (returns nil, caller reconnects).
func (d *device) session(ctx context.Context, conn *websocket.Conn) error {
	defer conn.Close()
	acks := make(chan uint64, d.cfg.Window+8)
	dead := make(chan struct{})
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		defer close(dead)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var a pb.Ack
			if proto.Unmarshal(data, &a) == nil {
				select {
				case acks <- a.Sequence:
				case <-quit:
					return
				}
			}
		}
	}()

	lost := func() {
		d.disconnects++
		now := time.Now()
		if d.lostAt.IsZero() {
			d.lostAt = now
		}
		if i := d.tl.idx(now); i >= 0 {
			d.tl.drops[i].Add(1)
		}
	}
	write := func(r *pb.Reading) bool {
		b, _ := proto.Marshal(r)
		return conn.WriteMessage(websocket.BinaryMessage, b) == nil
	}

	// resend everything still unacked, in sequence order
	seqs := make([]uint64, 0, len(d.unacked))
	for s := range d.unacked {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(a, b int) bool { return seqs[a] < seqs[b] })
	for _, s := range seqs {
		if !write(d.bySeq[s]) {
			lost()
			return nil
		}
		d.unacked[s] = 1
		d.resent++
	}

	var interval time.Duration
	if d.cfg.Rate > 0 {
		interval = time.Duration(float64(time.Second) / d.cfg.Rate)
	}
	nextSend := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		if d.done() {
			conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
			return nil
		}
		canSend := d.next < len(d.stream.Delivery) && d.outstanding() < d.cfg.Window
		if canSend && !time.Now().Before(nextSend) {
			r := d.stream.Delivery[d.next]
			if d.bySeq[r.Sequence] == nil {
				r.TimestampMs = time.Now().UnixMilli() // first send time, for end-to-end latency at the consumer
			}
			d.bySeq[r.Sequence] = r
			d.unacked[r.Sequence]++
			d.next++
			d.sent++
			if !write(r) {
				lost()
				return nil
			}
			nextSend = nextSend.Add(interval)
			if interval > 0 && time.Since(nextSend) > time.Second {
				nextSend = time.Now() // do not burst to catch up after a long stall
			}
			continue
		}
		wait := time.Hour
		if canSend {
			wait = time.Until(nextSend)
		}
		timer.Reset(wait)
		select {
		case s := <-acks:
			d.ack(s)
		case <-dead:
			// apply acks that arrived before the close
		drainAcks:
			for {
				select {
				case s := <-acks:
					d.ack(s)
				default:
					break drainAcks
				}
			}
			if d.done() {
				return nil
			}
			lost()
			return nil
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (d *device) ack(seq uint64) {
	c, ok := d.unacked[seq]
	if !ok {
		return // an extra ack for a resent frame already acked
	}
	if c <= 1 {
		delete(d.unacked, seq)
	} else {
		d.unacked[seq] = c - 1
	}
	d.acked++
	now := time.Now()
	if !d.lostAt.IsZero() {
		d.gaps = append(d.gaps, now.Sub(d.lostAt))
		d.lostAt = time.Time{}
	}
	if i := d.tl.idx(now); i >= 0 {
		d.tl.acks[i].Add(1)
	}
}

// Percentile returns the p-th percentile (0..100) of sorted durations.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p / 100)
	return sorted[i]
}

// String summarizes the run in one line.
func (s AckedStats) String() string {
	return fmt.Sprintf("sent=%d resent=%d acked=%d disconnects=%d dial_failures=%d elapsed=%s",
		s.Sent, s.Resent, s.Acked, s.Disconnects, s.DialFailures, s.Elapsed.Round(time.Millisecond))
}
