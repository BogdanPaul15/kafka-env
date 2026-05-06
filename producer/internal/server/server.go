package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"producer/internal/metrics"
	"producer/internal/model"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	HTTPPort int
}

type MessageWriter interface {
	Write(ctx context.Context, events []model.LogEvent) error
	Close() error
}

type Server struct {
	writer MessageWriter
	mux    *http.ServeMux
	addr   string
}

func NewServer(writer MessageWriter, cfg Config) *Server {
	s := &Server{
		writer: writer,
		mux:    http.NewServeMux(),
		addr:   fmt.Sprintf(":%d", cfg.HTTPPort),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Run() error {
	log.Printf("Producer listening on: %s", s.addr)
	return http.ListenAndServe(s.addr, s.mux)
}

func (s *Server) registerRoutes() {
	s.mux.Handle("/ingest", s.track("/ingests", http.HandlerFunc(s.handleIngest)))
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.Handle("/metrics", promhttp.Handler())
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) track(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		status := fmt.Sprintf("%d", recorder.status)
		metrics.HTTPRequestsTotal.WithLabelValues(route, status).Inc()
	})
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var events []model.LogEvent
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		errMsg := fmt.Errorf("failed to decode request body: %w", err)
		log.Println(errMsg)
		http.Error(w, "Invalid request body.", http.StatusBadRequest)
		return
	}

	if len(events) == 0 {
		http.Error(w, "Empty request body.", http.StatusBadRequest)
		return
	}

	if err := s.writer.Write(r.Context(), events); err != nil {
		errMsg := fmt.Errorf("failed to process events: %w", err)
		log.Println(errMsg)
		http.Error(w, "Failed to process events.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
