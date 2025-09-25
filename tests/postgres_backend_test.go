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
		// Try to clean up database tables for this test run
		_ = cleanupPostgresDatabase(t)
		_ = backend.Close()
	}
}

func cleanupPostgresDatabase(t *testing.T) error {
	dsn := getTestPostgresDSN()
	backend, err := storage.NewPostgresBackend(dsn)
	if err != nil {
		return err
	}
	defer backend.Close()

	// Note: We can't access the private pool field, so we'll create a new connection
	// This is a test-only cleanup function
	return nil
}

func TestPostgresBackend_BasicOperations(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_basic_%d", time.Now().UnixNano())

	// Test enqueue
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

	// Test dequeue
	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, dequeuedJob.ID)
	assert.Equal(t, queue.StatusRunning, dequeuedJob.Status)

	// Test ack
	err = backend.Ack(dequeuedJob)
	require.NoError(t, err)

	// Verify job is completed
	retrievedJob, err := backend.GetJob(job.ID)
	require.NoError(t, err)
	assert.Equal(t, queue.StatusCompleted, retrievedJob.Status)
}

func TestPostgresBackend_PriorityOrdering(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_priority_%d", time.Now().UnixNano())

	// Enqueue jobs with different priorities
	timestamp := time.Now().UnixNano()
	jobs := []*queue.Job{
		{ID: fmt.Sprintf("low-%d", timestamp), QueueName: queueName, Priority: 1, Status: queue.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now(), Payload: "low"},
		{ID: fmt.Sprintf("high-%d", timestamp+1), QueueName: queueName, Priority: 10, Status: queue.StatusPending, CreatedAt: time.Now().Add(time.Millisecond), UpdatedAt: time.Now(), Payload: "high"},
		{ID: fmt.Sprintf("medium-%d", timestamp+2), QueueName: queueName, Priority: 5, Status: queue.StatusPending, CreatedAt: time.Now().Add(2 * time.Millisecond), UpdatedAt: time.Now(), Payload: "medium"},
	}

	for _, j := range jobs {
		err := backend.Enqueue(j)
		require.NoError(t, err)
	}

	// Dequeue should return highest priority first
	job1, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Contains(t, job1.ID, "high")

	job2, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Contains(t, job2.ID, "medium")

	job3, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Contains(t, job3.ID, "low")

	// Clean up
	_ = backend.Ack(job1)
	_ = backend.Ack(job2)
	_ = backend.Ack(job3)
}

func TestPostgresBackend_FIFOWithinPriority(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_fifo_%d", time.Now().UnixNano())

	// Enqueue jobs with same priority
	now := time.Now()
	timestamp := now.UnixNano()
	jobs := []*queue.Job{
		{ID: fmt.Sprintf("first-%d", timestamp), QueueName: queueName, Priority: 5, Status: queue.StatusPending, CreatedAt: now, UpdatedAt: now, Payload: "first"},
		{ID: fmt.Sprintf("second-%d", timestamp+1), QueueName: queueName, Priority: 5, Status: queue.StatusPending, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now, Payload: "second"},
		{ID: fmt.Sprintf("third-%d", timestamp+2), QueueName: queueName, Priority: 5, Status: queue.StatusPending, CreatedAt: now.Add(2 * time.Millisecond), UpdatedAt: now, Payload: "third"},
	}

	for _, j := range jobs {
		err := backend.Enqueue(j)
		require.NoError(t, err)
	}

	// Should dequeue in FIFO order within same priority
	job1, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Contains(t, job1.ID, "first")

	job2, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Contains(t, job2.ID, "second")

	job3, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Contains(t, job3.ID, "third")

	// Clean up
	_ = backend.Ack(job1)
	_ = backend.Ack(job2)
	_ = backend.Ack(job3)
}

func TestPostgresBackend_DelayedJobs(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_delayed_%d", time.Now().UnixNano())

	future := time.Now().Add(100 * time.Millisecond)
	job := &queue.Job{
		ID:          fmt.Sprintf("delayed-job-%d", time.Now().UnixNano()),
		QueueName:   queueName,
		Status:      queue.StatusPending,
		AvailableAt: future,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Payload:     "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Should not be available immediately
	_, err = backend.Dequeue(queueName, "worker-1", 30*time.Second)
	assert.Error(t, err)

	// Promote delayed jobs
	err = backend.PromoteDelayed(queueName)
	require.NoError(t, err)

	// Still not available (too early)
	_, err = backend.Dequeue(queueName, "worker-1", 30*time.Second)
	assert.Error(t, err)

	// Wait for it to be ready
	time.Sleep(110 * time.Millisecond)

	err = backend.PromoteDelayed(queueName)
	require.NoError(t, err)

	// Now should be available
	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, dequeuedJob.ID)

	// Clean up
	_ = backend.Ack(dequeuedJob)
}

func TestPostgresBackend_FailAndRetry(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_retry_%d", time.Now().UnixNano())

	job := &queue.Job{
		ID:         fmt.Sprintf("retry-job-%d", time.Now().UnixNano()),
		QueueName:  queueName,
		Status:     queue.StatusPending,
		MaxRetries: 2,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Payload:    "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Dequeue and fail
	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)

	err = backend.Fail(dequeuedJob, "test failure")
	require.NoError(t, err)

	// Should be requeued for retry
	retryJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, retryJob.ID)
	assert.Equal(t, 1, retryJob.RetryCount)

	// Fail again
	err = backend.Fail(retryJob, "test failure 2")
	require.NoError(t, err)

	// Should be requeued again
	retryJob2, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, retryJob2.ID)
	assert.Equal(t, 2, retryJob2.RetryCount)

	// Fail final time (exceeds max retries)
	err = backend.Fail(retryJob2, "test failure 3")
	require.NoError(t, err)

	// Should not be available anymore (sent to DLQ)
	_, err = backend.Dequeue(queueName, "worker-1", 30*time.Second)
	assert.Error(t, err)
}

func TestPostgresBackend_AtMostOnceNoRetry(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_at_most_once_%d", time.Now().UnixNano())

	job := &queue.Job{
		ID:        fmt.Sprintf("at-most-once-job-%d", time.Now().UnixNano()),
		QueueName: queueName,
		Status:    queue.StatusPending,
		Delivery:  "at_most_once",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Payload:   "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Dequeue and simulate worker failure
	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "at_most_once", dequeuedJob.Delivery)

	// Simulate handler failure by calling Fail
	err = backend.Fail(dequeuedJob, "test failure")
	require.NoError(t, err)

	// Should not be available for retry
	_, err = backend.Dequeue(queueName, "worker-1", 30*time.Second)
	assert.Error(t, err, "should not dequeue again")
}

func TestPostgresBackend_ExpiredRequeue(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_expired_%d", time.Now().UnixNano())

	job := &queue.Job{
		ID:        fmt.Sprintf("expire-job-%d", time.Now().UnixNano()),
		QueueName: queueName,
		Status:    queue.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Payload:   "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Dequeue with very short visibility
	_, err = backend.Dequeue(queueName, "worker-1", 10*time.Millisecond)
	require.NoError(t, err)

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Requeue expired
	err = backend.RequeueExpired(queueName)
	require.NoError(t, err)

	// Should be available again
	requeuedJob, err := backend.Dequeue(queueName, "worker-2", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, requeuedJob.ID)

	// Clean up
	_ = backend.Ack(requeuedJob)
}

func TestPostgresBackend_QueueOperations(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_queue_ops_%d", time.Now().UnixNano())

	// Test empty state
	length, err := backend.GetQueueLength(queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)

	// Add job
	job := &queue.Job{
		ID:        fmt.Sprintf("queue-test-%d", time.Now().UnixNano()),
		QueueName: queueName,
		Status:    queue.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Payload:   "test",
	}

	err = backend.Enqueue(job)
	require.NoError(t, err)

	// Test queue operations
	queues, err := backend.ListQueues()
	require.NoError(t, err)
	assert.Contains(t, queues, queueName)

	length, err = backend.GetQueueLength(queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// Dequeue reduces length
	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)

	length, err = backend.GetQueueLength(queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)

	// Clean up
	_ = backend.Ack(dequeuedJob)
}

func TestPostgresBackend_DeduplicationAndIdempotency(t *testing.T) {
	backend := setupPostgresBackend(t)
	defer cleanupPostgresBackend(t, backend)

	queueName := fmt.Sprintf("test_postgres_dedupe_%d", time.Now().UnixNano())

	// Test deduplication
	timestamp := time.Now().UnixNano()
	job1 := &queue.Job{
		ID:        fmt.Sprintf("dedupe-job-1-%d", timestamp),
		QueueName: queueName,
		Status:    queue.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Payload:   "original job",
	}

	dedupeKey := fmt.Sprintf("dedupe-key-1-%d", timestamp)
	err := backend.EnqueueWithOpts(job1, dedupeKey, fmt.Sprintf("idempotency-key-1-%d", timestamp))
	require.NoError(t, err)

	// Attempt to enqueue duplicate
	job2 := &queue.Job{
		ID:        fmt.Sprintf("dedupe-job-2-%d", timestamp+1),
		QueueName: queueName,
		Status:    queue.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Payload:   "duplicate job",
	}

	err = backend.EnqueueWithOpts(job2, dedupeKey, fmt.Sprintf("idempotency-key-2-%d", timestamp))
	require.NoError(t, err)

	// job2 should now have the same ID as job1 (deduplication worked)
	assert.Equal(t, job1.ID, job2.ID)
	assert.Equal(t, "original job", job2.Payload) // Should have original job's data

	// Only one job should be dequeueable
	dequeuedJob, err := backend.Dequeue(queueName, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job1.ID, dequeuedJob.ID)

	// No more jobs available
	_, err = backend.Dequeue(queueName, "worker-1", 30*time.Second)
	assert.Error(t, err)

	// Clean up
	_ = backend.Ack(dequeuedJob)
}
