package storage

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"consumer/model"

	"github.com/opensearch-project/opensearch-go/v2"
)

type mockTransport struct {
	roundTripFunc func(req *http.Request) *http.Response
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req), nil
}

func TestStore_Success(t *testing.T) {
	fakeNet := &mockTransport{
		roundTripFunc: func(req *http.Request) *http.Response {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{"errors":false,"items":[]}`)),
				Header:     make(http.Header),
			}
		},
	}

	cfg := opensearch.Config{Transport: fakeNet}
	client, _ := opensearch.NewClient(cfg)
	storage := &OpenSearchClient{client: client}

	event := model.LogEvent{
		Timestamp: "2026-04-01 22:24:11.483837841",
		Level:     "ERROR",
		Service:   "payment-service",
		TraceID:   "abcd-efgh-ijkl-mnop",
		Message:   "test payment",
	}

	err := storage.Store(context.Background(), []model.LogEvent{event})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStore_OpensearchError(t *testing.T) {
	fakeNet := &mockTransport{
		roundTripFunc: func(req *http.Request) *http.Response {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(strings.NewReader(`{"error":"bad data"}`)),
				Header:     make(http.Header),
			}
		},
	}

	cfg := opensearch.Config{Transport: fakeNet}
	client, _ := opensearch.NewClient(cfg)
	storage := &OpenSearchClient{client: client}

	err := storage.Store(context.Background(), []model.LogEvent{{Timestamp: "invalid"}})

	if err == nil {
		t.Error("expected an error from OpenSearch, got nil")
	}
}
