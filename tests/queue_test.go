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

func TestPriorityOrderingFIFOWithinPriority(t *testing.T) {
	q, name := newTestQueueWithName()
	// Enqueue low priority first
	j1 := queue.NewJob("low-1", name)
	j1.Priority = 1
	_ = q.Enqueue(j1)
	// Then higher priority
	j2 := queue.NewJob("high", name)
	j2.Priority = 10
	_ = q.Enqueue(j2)
	// Then another low with later time
	time.Sleep(2 * time.Millisecond)
	j3 := queue.NewJob("low-2", name)
	j3.Priority = 1
	_ = q.Enqueue(j3)

	d1, _ := q.Dequeue(name)
	assert.Equal(t, j2.ID, d1.ID, "higher priority should dequeue first")
	_ = q.Ack(d1)
	d2, _ := q.Dequeue(name)
	assert.Equal(t, j1.ID, d2.ID, "older within same priority first")
	_ = q.Ack(d2)
	d3, _ := q.Dequeue(name)
	assert.Equal(t, j3.ID, d3.ID)
	_ = q.Ack(d3)
}

func TestDelayedJobPromotion(t *testing.T) {
	q, name := newTestQueueWithName()
	j := queue.NewJob("delayed", name)
	j.Priority = 5
	j.AvailableAt = time.Now().Add(1 * time.Second)
	_ = q.Enqueue(j)

	// Should not dequeue before ready
	if _, err := q.Dequeue(name); err == nil {
		t.Fatalf("expected empty before delay elapsed")
	}
	time.Sleep(1100 * time.Millisecond)
	// Promote explicitly (simulating ticker)
	if rq, ok := q.(*storage.RedisQueue); ok {
		_ = rq.PromoteDelayed(name)
	}
	d, err := q.Dequeue(name)
	if err != nil {
		t.Fatalf("dequeue after promotion: %v", err)
	}
	if d.ID != j.ID {
		t.Fatalf("expected job %s got %s", j.ID, d.ID)
	}
}

func TestRecoverySweepDeadWorker(t *testing.T) {
	q, name := newTestQueueWithName()

	// Enqueue and dequeue a job to move it to processing
	job := queue.NewJob("owned-by-dead-worker", name)
	assert.NoError(t, q.Enqueue(job))
	deq, err := q.Dequeue(name)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, deq.ID)

	// Simulate ownership by a worker and then simulate worker death by deleting heartbeat
	workerID := fmt.Sprintf("dead-%d", time.Now().UnixNano())
	_, _ = q.Client().HSet(q.Ctx(), "job:ownership", job.ID, workerID).Result()
	// Ensure no heartbeat exists for the worker (delete just in case)
	_, _ = q.Client().Del(q.Ctx(), fmt.Sprintf("worker:%s", workerID)).Result()

	// Run recovery sweep — should requeue immediately due to missing heartbeat
	assert.NoError(t, q.RequeueExpired(name))

	// Now the job should be back in the queue and dequeuable again
	requeued, err := q.Dequeue(name)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, requeued.ID)
}
