// gateway runs the WebSocket ingest edge (dedup, produce-or-spool, acks) as a
// standalone service, with the IngestAdmin and grpc.health.v1 services on a
// separate admin port. This is the binary deploy/k8s and deploy/aws run.
//
// On SIGTERM it cordons itself (readiness NOT_SERVING, device connections
// closed so devices reconnect to another replica), drains its spool one last
// time, and exits. Anything still spooled stays in the spool file, which the
// replica drains when it comes back with the same volume.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Manas103/event-ingestion-platform/internal/admin"
	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	"github.com/Manas103/event-ingestion-platform/internal/kafkaclient"
	"github.com/Manas103/event-ingestion-platform/internal/wsserver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "device (WebSocket) listen address")
	adminAddr := flag.String("admin-addr", "127.0.0.1:9090", "gRPC admin and health listen address")
	brokers := flag.String("brokers", "127.0.0.1:9092", "comma-separated Kafka bootstrap brokers")
	broker := flag.String("broker", "", "deprecated alias for -brokers")
	topic := flag.String("topic", "telemetry", "kafka topic")
	acksAll := flag.Bool("acks-all", false, "wait for all in-sync replicas on every produce")
	createTopic := flag.Bool("create-topic", false, "create the topic if missing (see -partitions, -replication, -min-isr)")
	partitions := flag.Int("partitions", 6, "partitions when creating the topic")
	replication := flag.Int("replication", 1, "replication factor when creating the topic")
	minISR := flag.Int("min-isr", 1, "min.insync.replicas when creating the topic")
	produceTimeout := flag.Duration("produce-timeout", 200*time.Millisecond, "one produce attempt before spooling")
	spoolPath := flag.String("spool", "spool.dat", "path to the backpressure spool file")
	spoolSync := flag.Bool("spool-fsync", false, "fsync every spooled record before acking it")
	drainInterval := flag.Duration("drain-interval", 1*time.Second, "how often to retry spooled readings")
	replica := flag.String("replica", os.Getenv("POD_NAME"), "replica name reported by the admin API")
	flag.Parse()

	if *broker != "" {
		*brokers = *broker
	}
	brokerList := kafkaclient.SplitBrokers(*brokers)
	if *createTopic {
		for attempt := 1; ; attempt++ {
			err := kafkaclient.EnsureReplicatedTopic(brokerList, *topic, *partitions, *replication, *minISR)
			if err == nil {
				break
			}
			if attempt == 60 {
				log.Fatalf("create topic: %v", err)
			}
			log.Printf("create topic (attempt %d): %v", attempt, err)
			time.Sleep(2 * time.Second)
		}
	}

	producer := kafkaclient.NewClusterProducer(kafkaclient.ProducerConfig{
		Brokers: brokerList, Topic: *topic, AcksAll: *acksAll,
	})
	defer producer.Close()

	spool, err := ingest.NewSpool(*spoolPath)
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	defer spool.Close()
	spool.SetSync(*spoolSync)

	pipeline := ingest.NewPipeline(ingest.NewDedup(), spool, producer)
	pipeline.ProduceTimeout = *produceTimeout
	edge := wsserver.NewServer(pipeline)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipeline.RunDrainer(ctx, *drainInterval)

	adminLn, err := net.Listen("tcp", *adminAddr)
	if err != nil {
		log.Fatalf("admin listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	svc := &admin.Service{Replica: *replica, Pipeline: pipeline, Edge: edge}
	admin.Register(grpcServer, svc)
	go grpcServer.Serve(adminLn)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	httpServer := &http.Server{Handler: edge.Handler()}
	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()
	log.Printf("gateway %s: devices on %s, admin on %s, kafka %v topic %q acks-all=%v",
		*replica, listener.Addr(), adminLn.Addr(), brokerList, *topic, *acksAll)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig

	start := time.Now()
	closed, _ := svc.Cordon(ctx, nil)
	shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	_ = httpServer.Shutdown(shutdownCtx)
	drained, respooled := pipeline.DrainOnce(shutdownCtx)
	grpcServer.GracefulStop()
	log.Printf("gateway %s: shut down in %s (closed %d connections, final drain %d produced, %d left spooled)",
		*replica, time.Since(start).Round(time.Millisecond), closed.ClosedConnections, drained, respooled)
}
