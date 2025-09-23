package tests

import (
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestQueueWithName() (queue.Queue, string) {
	name := fmt.Sprintf("test_jobs:%d", time.Now().UnixNano())
	return storage.NewRedisQueue("localhost:6379", name), name
}

func TestEnqueueDequeue(t *testing.T) {
	q, name := newTestQueueWithName()
	job := queue.NewJob("hello", name)
	err := q.Enqueue(job)
	assert.NoError(t, err)

	deq, err := q.Dequeue(name)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, deq.ID)
	assert.Equal(t, queue.StatusRunning, deq.Status)
}

func TestAck(t *testing.T) {
	q, name := newTestQueueWithName()

	job := queue.NewJob("process-me", name)
	_ = q.Enqueue(job)

	deq, _ := q.Dequeue(name)
	err := q.Ack(deq)
	assert.NoError(t, err)

	updated, _ := q.GetJob(deq.ID)
	assert.Equal(t, queue.StatusCompleted, updated.Status)
}

func TestFailAndRetry(t *testing.T) {
	q, name := newTestQueueWithName()

	job := queue.NewJob("fail-me", name)
	job.MaxRetries = 2
	_ = q.Enqueue(job)

	deq, _ := q.Dequeue(name)
	err := q.Fail(deq, "simulated failure")
	assert.NoError(t, err)

	// immediately after fail, should go into delayed set
	updated, _ := q.GetJob(deq.ID)
	assert.Equal(t, queue.StatusFailed, updated.Status)
	assert.Equal(t, 1, updated.RetryCount)

	// fast-forward requeue (simulate expired delay)
	_ = q.RequeueExpired(name)
	retryJob, _ := q.Dequeue(name)
	assert.Equal(t, job.ID, retryJob.ID)
}

func TestDeadLetterQueue(t *testing.T) {
	q, name := newTestQueueWithName()

	job := queue.NewJob("too-many-retries", name)
	job.MaxRetries = 1
	_ = q.Enqueue(job)

	//1st attempt
	deq, _ := q.Dequeue(name)
	_ = q.Fail(deq, "error 1")

	//2nd attempt - should go to DLQ
	deq2, _ := q.Dequeue(name)
	_ = q.Fail(deq2, "error 2")

	updated, _ := q.GetJob(deq2.ID)
	assert.Equal(t, queue.StatusFailed, updated.Status)
	assert.Equal(t, 2, updated.RetryCount)

	// Verify job in DLQ
	dlqJobs, _ := q.Client().LRange(q.Ctx(), "jobs:dlq", 0, -1).Result()
	assert.Contains(t, dlqJobs, job.ID)
}

func TestVisibilityTimeout(t *testing.T) {
	q, name := newTestQueueWithName()

	job := queue.NewJob("visibility-timeout", name)
	_ = q.Enqueue(job)

	deq, _ := q.Dequeue(name)
	assert.Equal(t, queue.StatusRunning, deq.Status)

	// dont ACK let  it expire
	time.Sleep(2 * time.Second)

	_ = q.RequeueExpired(name)
	requeue, _ := q.Dequeue(name)
	assert.Equal(t, job.ID, requeue.ID)
	assert.Equal(t, queue.StatusRunning, requeue.Status)
}

func TestMultipleQueuesProcessIndependently(t *testing.T) {
	q1, name1 := newTestQueueWithName()
	q2, name2 := newTestQueueWithName()

	job1 := queue.NewJob("job-queue-1", name1)
	job2 := queue.NewJob("job-queue-2", name2)
	// Enqueue jobs into their respective queues
	assert.NoError(t, q1.Enqueue(job1))
	assert.NoError(t, q2.Enqueue(job2))

	type result struct {
		job *queue.Job
		err error
	}
	r1 := make(chan result, 1)
	r2 := make(chan result, 1)

	var wg sync.WaitGroup
	wg.Add(2)

	// Simulate two workers servicing different queues concurrently
	go func() {
		defer wg.Done()
		j, err := q1.Dequeue(name1)
		if err == nil {
			_ = q1.Ack(j)
		}
		r1 <- result{j, err}
	}()
	go func() {
		defer wg.Done()
		j, err := q2.Dequeue(name2)
		if err == nil {
			_ = q2.Ack(j)
		}
		r2 <- result{j, err}
	}()

	wg.Wait()
	res1 := <-r1
	res2 := <-r2

	assert.NoError(t, res1.err)
	assert.NoError(t, res2.err)
	assert.Equal(t, job1.ID, res1.job.ID)
	assert.Equal(t, job2.ID, res2.job.ID)

	// Verify both jobs are marked completed
	upd1, _ := q1.GetJob(job1.ID)
	upd2, _ := q2.GetJob(job2.ID)
	assert.Equal(t, queue.StatusCompleted, upd1.Status)
	assert.Equal(t, queue.StatusCompleted, upd2.Status)
}
