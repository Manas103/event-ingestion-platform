// Package locationwsserver is the driver-facing edge for the location-ping
// extension: one WebSocket connection per simulated device, each binary
// frame a single serialized protobuf LocationPing.
package locationwsserver

import (
	"context"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/Manas103/event-ingestion-platform/internal/locationping"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Server struct {
	pipeline *locationping.Pipeline
}

func NewServer(p *locationping.Pipeline) *Server {
	return &Server{pipeline: p}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", s.handleIngest)
	return mux
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var ping pb.LocationPing
		if err := proto.Unmarshal(data, &ping); err != nil {
			log.Printf("locationwsserver: dropping unparseable frame: %v", err)
			continue
		}
		if err := s.pipeline.Ingest(context.Background(), &ping); err != nil {
			log.Printf("locationwsserver: ingest failed for %s/%d: %v", ping.DeviceId, ping.Sequence, err)
		}
	}
}
