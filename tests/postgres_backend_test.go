package tests

import (
"distributed-job-queue/pkg/queue"
"distributed-job-queue/pkg/storage"
"fmt"
"os"
"testing"
"time"

"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

func getTestPostgresDSN() string {
dsn := os.Getenv("TEST_POSTGRES_DSN")
if dsn == "" {
dsn = "postgres://postgres:postgres@localhost:5432/job_queue_test?sslmode=disable"
}
return dsn
}

func setupPostgresBackend(t *testing.T) *storage.PostgresBackend {
backend, err := storage.NewPostgresBackend(getTestPostgresDSN())
if err != nil {
t.Skipf("PostgreSQL not available, skipping test: %v", err)
}
return backend
}

func cleanupPostgresBackend(t *testing.T, backend *storage.PostgresBackend) {
if backend != nil {
_ = backend.Close()
}
}

func TestPostgresBackend_BasicOperations(t *testing.T) {
backend := setupPostgresBackend(t)
defer cleanupPostgresBackend(t, backend)

queueName := fmt.Sprintf("test_postgres_basic_%d", time.Now().UnixNano())

job := &queue.Job{
ID:        fmt.Sprintf("postgres-test-job-1-%d", time.Now().UnixNano()),
QueueName: queueName,
Status:    queue.StatusPending,
Payload:   "test payload",
CreatedAt: time.Now(),
UpdatedAt: time.Now(),
Priority:  5,
}

err := backend.Enqueue(job)
require.NoError(t, err)

dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
require.NoError(t, err)
assert.Equal(t, job.ID, dequeuedJob.ID)
assert.Equal(t, queue.StatusRunning, dequeuedJob.Status)

err = backend.Ack(dequeuedJob)
require.NoError(t, err)

retrievedJob, err := backend.GetJob(job.ID)
require.NoError(t, err)
assert.Equal(t, queue.StatusCompleted, retrievedJob.Status)
}
