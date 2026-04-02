package storage

import (
	"net/http"
)

type mockTransport struct {
	roundTripFunc func(req *http.Request) *http.Response
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req), nil
}
