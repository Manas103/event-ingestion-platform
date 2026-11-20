// Package wsserver is the device-facing edge: one WebSocket connection per
// device, each binary frame a single serialized protobuf Reading.
//
// A connection opened with ?ack=1 gets an Ack frame back for every Reading
// once the pipeline has made it durable (produced, spooled, or a duplicate of
// one that was). That is what makes a gateway crash lossless: the client
// resends whatever was not acked, to whichever replica it reconnects to.
// Connections without ?ack=1 keep the original fire-and-forget behavior the
// earlier benchmarks were measured with.
package wsserver

import (
	"context"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/Manas103/event-ingestion-platform/internal/ingest"
	pb "github.com/Manas103/event-ingestion-platform/proto"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Server accepts one WebSocket connection per device and feeds every
// protobuf-framed Reading it receives into the ingest pipeline.
type Server struct {
	pipeline *ingest.Pipeline

	mu       sync.Mutex
	conns    map[*websocket.Conn]struct{}
	cordoned bool

	AcksSent atomic.Uint64
}

func NewServer(p *ingest.Pipeline) *Server {
	return &Server{pipeline: p, conns: make(map[*websocket.Conn]struct{})}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", s.handleIngest)
	return mux
}

// OpenConnections is the number of device connections currently open.
func (s *Server) OpenConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Cordoned reports whether Cordon has been called.
func (s *Server) Cordoned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cordoned
}

// Cordon refuses new connections from now on and closes every open one, so
// devices reconnect elsewhere. Unacked readings on the closed connections are
// resent by their devices; nothing in flight is lost because a reading is only
// acked after the pipeline has made it durable. Returns how many connections
// were closed.
func (s *Server) Cordon() int {
	s.mu.Lock()
	s.cordoned = true
	open := make([]*websocket.Conn, 0, len(s.conns))
	for c := range s.conns {
		open = append(open, c)
	}
	s.mu.Unlock()
	for _, c := range open {
		_ = c.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "replica cordoned"), deadlineSoon())
		c.Close()
	}
	return len(open)
}

func (s *Server) track(c *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cordoned {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c *websocket.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if s.Cordoned() {
		http.Error(w, "replica cordoned", http.StatusServiceUnavailable)
		return
	}
	ackMode := r.URL.Query().Get("ack") == "1"
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	if !s.track(conn) {
		return
	}
	defer s.untrack(conn)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return // connection closed by the device, or by Cordon
		}
		var reading pb.Reading
		if err := proto.Unmarshal(data, &reading); err != nil {
			log.Printf("wsserver: dropping unparseable frame: %v", err)
			continue
		}
		if err := s.pipeline.Ingest(context.Background(), &reading); err != nil {
			log.Printf("wsserver: ingest failed for %s/%d: %v", reading.DeviceId, reading.Sequence, err)
			continue // not durable, so not acked: the device resends it
		}
		if ackMode {
			b, _ := proto.Marshal(&pb.Ack{Sequence: reading.Sequence})
			if err := conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
				return
			}
			s.AcksSent.Add(1)
		}
	}
}

func deadlineSoon() time.Time { return time.Now().Add(time.Second) }
