package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueLength = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "queue_length",
			Help: "Current number of jobs queued per queue",
		},
		[]string{"queue"},
	)

	JobsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Total number of jobs processed by status",
		},
		[]string{"status", "queue"},
	)

	WorkerHeartbeatsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "worker_heartbeats_total",
			Help: "Total number of worker heartbeats sent",
		},
	)

	JobLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "job_latency_seconds",
			Help:    "Time from enqueue to completion (ack/fail)",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"queue"},
	)

	DedupeEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_dedupe_total",
			Help: "Dedupe events (outcome=new|hit)",
		},
		[]string{"outcome"},
	)
)

// ObserveJobCompletion records job latency from created_at to now
func ObserveJobCompletion(queue string, createdAt time.Time) {
	JobLatency.WithLabelValues(queue).Observe(time.Since(createdAt).Seconds())
}
