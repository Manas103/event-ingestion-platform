// eipctl is the operator client for the gateway admin API (gRPC).
//
//	eipctl -targets=gw-0:9090,gw-1:9090 stats    one row of counters per replica
//	eipctl -targets=gw-0:9090 drain              retry the spool now
//	eipctl -targets=gw-0:9090 cordon             stop accepting, close device connections
//	eipctl -targets=gw-0:9090 health             grpc.health.v1 status
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/Manas103/event-ingestion-platform/proto/adminpb"
)

func main() {
	targets := flag.String("targets", "127.0.0.1:9090", "comma-separated admin addresses")
	flag.Parse()
	cmd := flag.Arg(0)
	if cmd == "" {
		cmd = "stats"
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	if cmd == "stats" {
		fmt.Fprintln(tw, "replica\treceived\tduplicates\tproduced\tspooled\tdrained\tacks\tspool_bytes\tdedup_entries\tconns\tcordoned\t")
	}
	failed := false
	for _, t := range strings.Split(*targets, ",") {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.NewClient(t, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("%s: %v", t, err)
		}
		c := adminpb.NewIngestAdminClient(conn)
		switch cmd {
		case "stats":
			s, err := c.Stats(ctx, &adminpb.StatsRequest{})
			if err != nil {
				fmt.Fprintf(tw, "%s\terror: %v\t\n", t, err)
				failed = true
				break
			}
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%v\t\n", s.Replica, s.Received, s.Duplicates,
				s.Produced, s.Spooled, s.Drained, s.AcksSent, s.SpoolBytes, s.DedupEntries, s.OpenConnections, s.Cordoned)
		case "drain":
			r, err := c.DrainSpool(ctx, &adminpb.DrainSpoolRequest{})
			if err != nil {
				log.Fatalf("%s: %v", t, err)
			}
			fmt.Printf("%s: produced %d, still spooled %d\n", t, r.Drained, r.Respooled)
		case "cordon":
			r, err := c.Cordon(ctx, &adminpb.CordonRequest{})
			if err != nil {
				log.Fatalf("%s: %v", t, err)
			}
			fmt.Printf("%s: cordoned, closed %d device connections\n", t, r.ClosedConnections)
		case "health":
			h, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			if err != nil {
				log.Fatalf("%s: %v", t, err)
			}
			fmt.Printf("%s: %s\n", t, h.Status)
		default:
			log.Fatalf("unknown command %q", cmd)
		}
		cancel()
		conn.Close()
	}
	tw.Flush()
	if failed {
		os.Exit(1)
	}
}
