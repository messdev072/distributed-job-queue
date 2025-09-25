package tests

import (
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluggableBackends(t *testing.T) {
	backends := map[string]storage.Backend{
		"Redis":  storage.NewRedisBackend("localhost:6379"),
		"Memory": storage.NewMemoryBackend(),
	}

	// Add PostgresBackend if available
	if postgresBackend, err := storage.NewPostgresBackend("postgres://postgres:postgres@localhost:5432/job_queue_test?sslmode=disable"); err == nil {
		backends["Postgres"] = postgresBackend
	}

	for name, backend := range backends {
		t.Run(name, func(t *testing.T) {
			testBackendOperations(t, backend, fmt.Sprintf("pluggable_test_%s_%d", name, time.Now().UnixNano()))
		})
	}
}

func testBackendOperations(t *testing.T, backend storage.Backend, queueName string) {
	// Test basic enqueue/dequeue
	job := &queue.Job{
		ID:        fmt.Sprintf("test-job-1-%d-%d", time.Now().UnixNano(), rand.Int63()),
		QueueName: queueName,
		Status:    queue.StatusPending,
		Payload:   "test payload",
		CreatedAt: time.Now(),
		Priority:  5,
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, dequeuedJob.ID)
	assert.Equal(t, queue.StatusRunning, dequeuedJob.Status)

	// Test ack
	err = backend.Ack(dequeuedJob)
	require.NoError(t, err)

	retrievedJob, err := backend.GetJob(job.ID)
	require.NoError(t, err)
	assert.Equal(t, queue.StatusCompleted, retrievedJob.Status)

	// Test priority ordering
	jobs := []*queue.Job{
		{ID: "low", QueueName: queueName, Priority: 1, Status: queue.StatusPending, CreatedAt: time.Now(), Payload: "low"},
		{ID: "high", QueueName: queueName, Priority: 10, Status: queue.StatusPending, CreatedAt: time.Now().Add(time.Millisecond), Payload: "high"},
		{ID: "medium", QueueName: queueName, Priority: 5, Status: queue.StatusPending, CreatedAt: time.Now().Add(2 * time.Millisecond), Payload: "medium"},
	}

	for _, j := range jobs {
		err := backend.Enqueue(j)
		require.NoError(t, err)
	}

	// Should dequeue in priority order: high, medium, low
	job1, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "high", job1.ID)

	job2, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "medium", job2.ID)

	job3, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "low", job3.ID)

	// Test queue operations
	queues, err := backend.ListQueues()
	require.NoError(t, err)
	assert.Contains(t, queues, queueName)

	// Ack remaining jobs to clean up
	_ = backend.Ack(job1)
	_ = backend.Ack(job2)
	_ = backend.Ack(job3)
}
