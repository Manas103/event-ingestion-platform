// Package admin is the gateway's operator API: the IngestAdmin gRPC service
// (stats, drain the spool now, cordon) plus the standard grpc.health.v1
// service, which the Kubernetes readiness and liveness probes call directly.
package admin

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/wsserver"
	"github.com/Manas103/event-ingestion-platform/proto/adminpb"
)

// Service implements adminpb.IngestAdminServer for one gateway replica.
type Service struct {
	adminpb.UnimplementedIngestAdminServer
	Replica  string
	Pipeline *ingest.Pipeline
	Edge     *wsserver.Server
	Health   *health.Server
}

// Register adds IngestAdmin and grpc.health.v1 to s and marks the replica
// SERVING. The empty service name "" is what a Kubernetes grpc probe checks.
func Register(s *grpc.Server, svc *Service) {
	if svc.Health == nil {
		svc.Health = health.NewServer()
	}
	adminpb.RegisterIngestAdminServer(s, svc)
	healthpb.RegisterHealthServer(s, svc.Health)
	svc.Health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}

func (s *Service) Stats(context.Context, *adminpb.StatsRequest) (*adminpb.StatsReply, error) {
	c := &s.Pipeline.Counters
	size, _ := s.Pipeline.Spool().SizeBytes()
	return &adminpb.StatsReply{
		Replica:         s.Replica,
		Received:        c.Received.Load(),
		Duplicates:      c.Duplicates.Load(),
		Produced:        c.Produced.Load(),
		Spooled:         c.Spooled.Load(),
		Drained:         c.Drained.Load(),
		AcksSent:        s.Edge.AcksSent.Load(),
		SpoolBytes:      size,
		DedupEntries:    uint64(s.Pipeline.Dedup().EntryCount()),
		DedupDevices:    uint64(s.Pipeline.Dedup().DeviceCount()),
		OpenConnections: int64(s.Edge.OpenConnections()),
		Cordoned:        s.Edge.Cordoned(),
	}, nil
}

func (s *Service) DrainSpool(ctx context.Context, _ *adminpb.DrainSpoolRequest) (*adminpb.DrainSpoolReply, error) {
	drained, respooled := s.Pipeline.DrainOnce(ctx)
	return &adminpb.DrainSpoolReply{Drained: uint64(drained), Respooled: uint64(respooled)}, nil
}

// Cordon turns readiness NOT_SERVING first (so the Service stops routing new
// connections here), then closes the open device connections.
func (s *Service) Cordon(context.Context, *adminpb.CordonRequest) (*adminpb.CordonReply, error) {
	s.Health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return &adminpb.CordonReply{ClosedConnections: int64(s.Edge.Cordon())}, nil
}
