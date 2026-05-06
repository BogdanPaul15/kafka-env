package storage

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"consumer/model"

	"github.com/opensearch-project/opensearch-go/v2"
)

type Config struct {
	Addresses []string
}

type OpenSearchClient struct {
	client *opensearch.Client
}

func NewOpenSearchClient(cfg Config) (*OpenSearchClient, error) {
	osCfg := opensearch.Config{
		Addresses: cfg.Addresses,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	client, err := opensearch.NewClient(osCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create opensearch client: %w", err)
	}

	return &OpenSearchClient{
		client: client,
	}, nil
}

func (o *OpenSearchClient) Store(ctx context.Context, events []model.LogEvent) error {
	var buf bytes.Buffer

	for _, event := range events {
		t, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			t = time.Now()
		}

		index := fmt.Sprintf("app-logs-%s", t.Format("2006.01.02"))

		meta := map[string]any{
			"index": map[string]any{
				"_index": index,
			},
		}
		if err := json.NewEncoder(&buf).Encode(meta); err != nil {
			return fmt.Errorf("failed to encode bulk meta: %w", err)
		}

		if err := json.NewEncoder(&buf).Encode(event); err != nil {
			return fmt.Errorf("failed to encode events: %w", err)
		}
	}

	resp, err := o.client.Bulk(&buf, o.client.Bulk.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return fmt.Errorf("bulk request completed with per-document errors")
	}

	return nil
}

func (o *OpenSearchClient) Close() error {
	return nil
}
