package tests

import (
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryBackend_BasicOperations(t *testing.T) {
	backend := storage.NewMemoryBackend()

	// Test enqueue
	job := &queue.Job{
		ID:        "test-job-1",
		QueueName: "test_queue",
		Status:    queue.StatusPending,
		Payload:   `{"key": "value"}`,
		CreatedAt: time.Now(),
		Priority:  5,
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Test dequeue
	dequeuedJob, err := backend.Dequeue("test_queue", "worker-1", 30*time.Second)
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

func TestMemoryBackend_PriorityOrdering(t *testing.T) {
	backend := storage.NewMemoryBackend()

	// Enqueue jobs with different priorities
	jobs := []*queue.Job{
		{ID: "low", QueueName: "priority_test", Priority: 1, Status: queue.StatusPending, CreatedAt: time.Now(), Payload: "test"},
		{ID: "high", QueueName: "priority_test", Priority: 10, Status: queue.StatusPending, CreatedAt: time.Now().Add(time.Millisecond), Payload: "test"},
		{ID: "medium", QueueName: "priority_test", Priority: 5, Status: queue.StatusPending, CreatedAt: time.Now().Add(2 * time.Millisecond), Payload: "test"},
	}

	for _, job := range jobs {
		err := backend.Enqueue(job)
		require.NoError(t, err)
	}

	// Dequeue should return highest priority first
	job1, err := backend.Dequeue("priority_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "high", job1.ID)

	job2, err := backend.Dequeue("priority_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "medium", job2.ID)

	job3, err := backend.Dequeue("priority_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "low", job3.ID)
}

func TestMemoryBackend_FIFOWithinPriority(t *testing.T) {
	backend := storage.NewMemoryBackend()

	// Enqueue jobs with same priority
	now := time.Now()
	jobs := []*queue.Job{
		{ID: "first", QueueName: "fifo_test", Priority: 5, Status: queue.StatusPending, CreatedAt: now, Payload: "test"},
		{ID: "second", QueueName: "fifo_test", Priority: 5, Status: queue.StatusPending, CreatedAt: now.Add(time.Millisecond), Payload: "test"},
		{ID: "third", QueueName: "fifo_test", Priority: 5, Status: queue.StatusPending, CreatedAt: now.Add(2 * time.Millisecond), Payload: "test"},
	}

	for _, job := range jobs {
		err := backend.Enqueue(job)
		require.NoError(t, err)
	}

	// Should dequeue in FIFO order within same priority
	job1, err := backend.Dequeue("fifo_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "first", job1.ID)

	job2, err := backend.Dequeue("fifo_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "second", job2.ID)

	job3, err := backend.Dequeue("fifo_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "third", job3.ID)
}

func TestMemoryBackend_DelayedJobs(t *testing.T) {
	backend := storage.NewMemoryBackend()

	future := time.Now().Add(100 * time.Millisecond)
	job := &queue.Job{
		ID:          "delayed-job",
		QueueName:   "delayed_test",
		Status:      queue.StatusPending,
		AvailableAt: future,
		CreatedAt:   time.Now(),
		Payload:     "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Should not be available immediately
	_, err = backend.Dequeue("delayed_test", "worker-1", 30*time.Second)
	assert.Error(t, err)

	// Promote delayed jobs
	err = backend.PromoteDelayed("delayed_test")
	require.NoError(t, err)

	// Still not available (too early)
	_, err = backend.Dequeue("delayed_test", "worker-1", 30*time.Second)
	assert.Error(t, err)

	// Wait for it to be ready
	time.Sleep(110 * time.Millisecond)

	err = backend.PromoteDelayed("delayed_test")
	require.NoError(t, err)

	// Now should be available
	dequeuedJob, err := backend.Dequeue("delayed_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, dequeuedJob.ID)
}

func TestMemoryBackend_FailAndRetry(t *testing.T) {
	backend := storage.NewMemoryBackend()

	job := &queue.Job{
		ID:         "retry-job",
		QueueName:  "retry_test",
		Status:     queue.StatusPending,
		MaxRetries: 2,
		CreatedAt:  time.Now(),
		Payload:    "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Dequeue and fail
	dequeuedJob, err := backend.Dequeue("retry_test", "worker-1", 30*time.Second)
	require.NoError(t, err)

	err = backend.Fail(dequeuedJob, "test failure")
	require.NoError(t, err)

	// Should be requeued for retry
	retryJob, err := backend.Dequeue("retry_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, retryJob.ID)
	assert.Equal(t, 1, retryJob.RetryCount)

	// Fail again
	err = backend.Fail(retryJob, "test failure 2")
	require.NoError(t, err)

	// Should be requeued again
	retryJob2, err := backend.Dequeue("retry_test", "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, retryJob2.ID)
	assert.Equal(t, 2, retryJob2.RetryCount)

	// Fail final time (exceeds max retries)
	err = backend.Fail(retryJob2, "test failure 3")
	require.NoError(t, err)

	// Should not be available anymore (sent to DLQ)
	_, err = backend.Dequeue("retry_test", "worker-1", 30*time.Second)
	assert.Error(t, err)
}

func TestMemoryBackend_ExpiredRequeue(t *testing.T) {
	backend := storage.NewMemoryBackend()

	job := &queue.Job{
		ID:        "expire-job",
		QueueName: "expire_test",
		Status:    queue.StatusPending,
		CreatedAt: time.Now(),
		Payload:   "test",
	}

	err := backend.Enqueue(job)
	require.NoError(t, err)

	// Dequeue with very short visibility
	_, err = backend.Dequeue("expire_test", "worker-1", 10*time.Millisecond)
	require.NoError(t, err)

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Requeue expired
	err = backend.RequeueExpired("expire_test")
	require.NoError(t, err)

	// Should be available again
	requeuedJob, err := backend.Dequeue("expire_test", "worker-2", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, job.ID, requeuedJob.ID)
}

func TestMemoryBackend_QueueOperations(t *testing.T) {
	backend := storage.NewMemoryBackend()

	// Test empty state
	queues, err := backend.ListQueues()
	require.NoError(t, err)
	assert.Empty(t, queues)

	length, err := backend.GetQueueLength("nonexistent")
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)

	// Add job
	job := &queue.Job{
		ID:        "queue-test",
		QueueName: "queue_ops_test",
		Status:    queue.StatusPending,
		CreatedAt: time.Now(),
		Payload:   "test",
	}

	err = backend.Enqueue(job)
	require.NoError(t, err)

	// Test queue operations
	queues, err = backend.ListQueues()
	require.NoError(t, err)
	assert.Contains(t, queues, "queue_ops_test")

	length, err = backend.GetQueueLength("queue_ops_test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// Dequeue reduces length
	_, err = backend.Dequeue("queue_ops_test", "worker-1", 30*time.Second)
	require.NoError(t, err)

	length, err = backend.GetQueueLength("queue_ops_test")
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)
}
