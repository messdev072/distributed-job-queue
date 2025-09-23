package tests

import (
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestQueue() queue.Queue {
	return storage.NewRedisQueue("localhost:6379", "test_jobs")
}

func TestEnqueueDequeue(t *testing.T) {
	q := newTestQueue()
	job := queue.NewJob("hello")
	err := q.Enqueue(job)
	assert.NoError(t, err)

	deq, err := q.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, job.ID, deq.ID)
	assert.Equal(t, queue.StatusRunning, deq.Status)
}

func TestAck(t *testing.T) {
	q := newTestQueue()

	job := queue.NewJob("process-me")
	_ = q.Enqueue(job)

	deq, _ := q.Dequeue()
	err := q.Ack(deq)
	assert.NoError(t, err)

	updated, _ := q.GetJob(deq.ID)
	assert.Equal(t, queue.StatusCompleted, updated.Status)
}

func TestFailAndRetry(t *testing.T) {
	q := newTestQueue()

	job := queue.NewJob("fail-me")
	job.MaxRetries = 2
	_ = q.Enqueue(job)

	deq, _ := q.Dequeue()
	err := q.Fail(deq, "simulated failure")
	assert.NoError(t, err)

	// immediately after fail, should go into delayed set
	updated, _ := q.GetJob(deq.ID)
	assert.Equal(t, queue.StatusFailed, updated.Status)
	assert.Equal(t, 1, updated.RetryCount)

	// fast-forward requeue (simulate expired delay)
	_ = q.RequeueExpired()
	retryJob, _ := q.Dequeue()
	assert.Equal(t, job.ID, retryJob.ID)
}

func TestDeadLetterQueue(t *testing.T) {
	q := newTestQueue()

	job := queue.NewJob("too-many-retries")
	job.MaxRetries = 1
	_ = q.Enqueue(job)

	//1st attempt
	deq, _ := q.Dequeue()
	_ = q.Fail(deq, "error 1")

	//2nd attempt - should go to DLQ
	deq2, _ := q.Dequeue()
	_ = q.Fail(deq2, "error 2")

	updated, _ := q.GetJob(deq2.ID)
	assert.Equal(t, queue.StatusFailed, updated.Status)
	assert.Equal(t, 2, updated.RetryCount)

	// Verify job in DLQ
	dlqJobs, _ := q.Client().LRange(q.Ctx(), "jobs:dlq", 0, -1).Result()
	assert.Contains(t, dlqJobs, job.ID)
}

func TestVisibilityTimeout(t *testing.T) {
	q := newTestQueue()

	job := queue.NewJob("visibility-timeout")
	_ = q.Enqueue(job)

	deq, _ := q.Dequeue()
	assert.Equal(t, queue.StatusRunning, deq.Status)

	// dont ACK let  it expire
	time.Sleep(2 * time.Second)

	_ = q.RequeueExpired()
	requeue, _ := q.Dequeue()
	assert.Equal(t, job.ID, requeue.ID)
	assert.Equal(t, queue.StatusRunning, requeue.Status)
}
