package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"producer/internal/model"
	"producer/internal/writer"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	HTTPPort int
}

type Server struct {
	writer writer.MessageWriter
	mux    *http.ServeMux
	addr   string
}

func NewServer(writer *writer.Writer, cfg Config) *Server {
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
	s.mux.HandleFunc("/ingest", s.handleIngest)
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.Handle("/metrics", promhttp.Handler())
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
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

	if err := s.writer.Write(r.Context(), events); err != nil {
		errMsg := fmt.Errorf("failed to process events: %w", err)
		log.Println(errMsg)
		http.Error(w, "Failed to process events.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
