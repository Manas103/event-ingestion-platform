package wsserver

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/simdevice"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

type memProducer struct {
	mu  sync.Mutex
	got []*pb.Reading
}

func (m *memProducer) Produce(_ context.Context, r *pb.Reading) error {
	m.mu.Lock()
	m.got = append(m.got, r)
	m.mu.Unlock()
	return nil
}

func startReplica(t *testing.T, prod *memProducer) (*Server, string) {
	t.Helper()
	spool, err := ingest.NewSpool(t.TempDir() + "/spool.dat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Close() })
	srv := NewServer(ingest.NewPipeline(ingest.NewDedup(), spool, prod))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: srv.Handler()}
	go hs.Serve(ln)
	t.Cleanup(func() { hs.Close() })
	return srv, "ws://" + ln.Addr().String() + "/ingest?ack=1"
}

// Two replicas; replica A is cordoned while devices are mid-stream. Every
// device must reconnect to B, resend what A had not acked, and finish, and
// the union of what both replicas produced must contain every unique reading
// (duplicates across replicas are expected: edge dedup is per replica).
func TestCordonMidStreamLosesNothing(t *testing.T) {
	var prodA, prodB memProducer
	a, urlA := startReplica(t, &prodA)
	_, urlB := startReplica(t, &prodB)

	streams := simdevice.BuildReplayCorpus(50, 200, 0.05, 7)
	go func() {
		time.Sleep(300 * time.Millisecond)
		a.Cordon()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// devices start round robin on A and B; A's devices fail over to B
	cfg := simdevice.AckedConfig{URLs: []string{urlA, urlB}, Rate: 400, Window: 16}
	stats, err := simdevice.SendAcked(ctx, streams, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Disconnects == 0 {
		t.Fatal("cordon closed no connections; the test did not exercise failover")
	}
	// a dropped device must move to the other replica first, not spend a
	// failed dial on the one that just closed it (found on the AWS run)
	if stats.DialFailures != 0 {
		t.Fatalf("%d failed redials after the cordon; devices redialed the cordoned replica", stats.DialFailures)
	}
	all := append(append([]*pb.Reading{}, prodA.got...), prodB.got...)
	want := ingest.FinalStateDigest(ingest.ReferenceDedup(simdevice.AllUnique(streams)))
	got := ingest.FinalStateDigest(ingest.ReferenceDedup(all))
	if string(want) != string(got) {
		t.Fatalf("final state after failover differs from the reference (A produced %d, B %d)", len(prodA.got), len(prodB.got))
	}
	t.Logf("%s; A produced %d, B produced %d", stats, len(prodA.got), len(prodB.got))
}
