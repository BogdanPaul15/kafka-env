package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"producer/internal/model"
)

type mockWriter struct {
	err    error
	called bool
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (m *mockWriter) Write(_ context.Context, _ []model.LogEvent) error {
	m.called = true
	return m.err
}

func (m *mockWriter) Close() error { return nil }

func newTestServer(t *testing.T, w *mockWriter) *Server {
	t.Helper()
	return NewServer(w, Config{HTTPPort: 0})
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t, &mockWriter{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestIngest(t *testing.T) {
	mock := &mockWriter{}
	s := newTestServer(t, mock)

	body, _ := json.Marshal([]model.LogEvent{
		{
			Timestamp: "2026-04-01 22:24:11.483837841",
			Level:     "INFO",
			Service:   "payment-service",
			TraceID:   "abcd-efgh-ijkl-mnop",
			Message:   "test payment",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if !mock.called {
		t.Fatal("expected writer.Write to be called")
	}
}

func TestIngest_WrongMethod(t *testing.T) {
	s := newTestServer(t, &mockWriter{})
	req := httptest.NewRequest(http.MethodGet, "/ingest", nil)
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestIngest_BadBody(t *testing.T) {
	s := newTestServer(t, &mockWriter{})
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBufferString("not-json"))
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestIngest_WriterError(t *testing.T) {
	mock := &mockWriter{err: errors.New("kafka down")}
	s := newTestServer(t, mock)

	body, _ := json.Marshal([]model.LogEvent{
		{
			Timestamp: "2026-04-01 22:24:11.483837841",
			Level:     "ERROR",
			Service:   "payment-service",
			TraceID:   "abcd-efgh-ijkl-mnop",
			Message:   "test payment",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
