// Package httpd provides the HTTP server for accessing the distributed key-value store.
// It also provides the endpoint for other nodes to join an existing cluster.
package httpd

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// IDGenerator is the interface Raft-backed key-value stores must implement.
type IDGenerator interface {
	// GetAll returns all allocated ids
	GetAll() []int

	// Acquire returns a new identifier, via distributed consensus.
	Acquire() (int, error)

	// Delete removes the given key, via distributed consensus.
	Release(id int) error

	// Join joins the node, identitifed by nodeID and reachable at addr, to the cluster.
	Join(nodeID string, addr string) error
}

// Service provides HTTP service.
type Service struct {
	addr string
	ln   net.Listener

	gen IDGenerator
}

// New returns an uninitialized HTTP service.
func New(addr string, gen IDGenerator) *Service {
	return &Service{
		addr: addr,
		gen:  gen,
	}
}

// Start starts the service.
func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.ln.Close()
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/teid") {
		s.handleTEIDRequest(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	nodeID, ok := m["id"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.gen.Join(nodeID, remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s *Service) handleTEIDRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() int {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 {
			return 0
		}
		atoi, err := strconv.Atoi(parts[2])
		if err != nil {
			return 0
		}
		return atoi
	}

	switch r.Method {
	case "GET":
		vals := s.gen.GetAll()
		slices.Sort(vals)
		b, err := json.Marshal(map[string][]int{"ids": vals})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, string(b))

	case "POST":
		v, err := s.gen.Acquire()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, err := json.Marshal(map[string]int{"id": v})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, string(b))

	case "DELETE":
		k := getKey()
		if k == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := s.gen.Release(k); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
