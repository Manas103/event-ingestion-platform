package admin

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/wsserver"
	pb "github.com/Manas103/event-ingestion-platform/proto"
	"github.com/Manas103/event-ingestion-platform/proto/adminpb"
)

type okProducer struct{}

func (okProducer) Produce(context.Context, *pb.Reading) error { return nil }

func TestStatsAndCordonOverGRPC(t *testing.T) {
	spool, err := ingest.NewSpool(t.TempDir() + "/spool.dat")
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	p := ingest.NewPipeline(ingest.NewDedup(), spool, okProducer{})
	for _, seq := range []uint64{1, 2, 2, 3} {
		p.Ingest(context.Background(), &pb.Reading{DeviceId: "d", Sequence: seq})
	}

	ln := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	Register(s, &Service{Replica: "gw-0", Pipeline: p, Edge: wsserver.NewServer(p)})
	go s.Serve(ln)
	defer s.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return ln.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	st, err := adminpb.NewIngestAdminClient(conn).Stats(ctx, &adminpb.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Replica != "gw-0" || st.Received != 4 || st.Duplicates != 1 || st.Produced != 3 || st.DedupEntries != 3 {
		t.Fatalf("unexpected stats: %+v", st)
	}

	health := healthpb.NewHealthClient(conn)
	h, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || h.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health before cordon = %v, %v", h, err)
	}
	if _, err := adminpb.NewIngestAdminClient(conn).Cordon(ctx, &adminpb.CordonRequest{}); err != nil {
		t.Fatal(err)
	}
	h, err = health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || h.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health after cordon = %v, %v", h, err)
	}
}
