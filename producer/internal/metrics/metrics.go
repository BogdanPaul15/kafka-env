package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "producer_http_requests_total",
		Help: "Total number of HTTP requests by route and status.",
	}, []string{"route", "status"})

	KafkaMessagesWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_kafka_messages_written_total",
		Help: "Total number of messages successfully written to Kafka.",
	})

	KafkaWriteErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_kafka_write_errors_total",
		Help: "Total number of failed Kafka write attempts.",
	})

	KafkaWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "producer_kafka_write_duration_seconds",
		Help:    "Latency of Kafka WriteMessages calls.",
		Buckets: prometheus.DefBuckets,
	})
)
