package tests

import (
	"context"
	q "distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type countingHooks struct {
	enq  int32
	ack  int32
	fail int32
}

func (h *countingHooks) OnEnqueue(ctx context.Context, job *q.Job) { atomic.AddInt32(&h.enq, 1) }
func (h *countingHooks) OnAck(ctx context.Context, job *q.Job)     { atomic.AddInt32(&h.ack, 1) }
func (h *countingHooks) OnFail(ctx context.Context, job *q.Job, reason string) {
	atomic.AddInt32(&h.fail, 1)
}

func TestLifecycleHooks_EnqueueAckFail(t *testing.T) {
	qname := fmt.Sprintf("test_hooks:%d", time.Now().UnixNano())
	rq := storage.NewRedisQueue("localhost:6379", qname)
	hooks := &countingHooks{}
	rq.WithHooks(hooks)

	job := q.NewJob("payload", qname)
	if err := rq.Enqueue(job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// dequeue
	j2, err := rq.Dequeue(qname)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if j2.ID != job.ID {
		t.Fatalf("expected same id")
	}

	// ack triggers OnAck
	if err := rq.Ack(j2); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// give goroutines a tick
	// not ideal but sufficient for async hook in unit test
	// could be improved with sync primitives if needed
	// small sleep to allow async goroutines to run
	// Using busy wait on atomics for portability
	for i := 0; i < 1000; i++ {
		if atomic.LoadInt32(&hooks.enq) > 0 && atomic.LoadInt32(&hooks.ack) > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	if atomic.LoadInt32(&hooks.enq) == 0 {
		t.Errorf("OnEnqueue not called")
	}
	if atomic.LoadInt32(&hooks.ack) == 0 {
		t.Errorf("OnAck not called")
	}

	// now simulate fail path
	job2 := q.NewJob("payload2", qname)
	job2.MaxRetries = 0 // force terminal fail on first failure
	if err := rq.Enqueue(job2); err != nil {
		t.Fatalf("enqueue2: %v", err)
	}
	if _, err := rq.Dequeue(qname); err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if err := rq.Fail(job2, "boom"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if atomic.LoadInt32(&hooks.fail) > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if atomic.LoadInt32(&hooks.fail) == 0 {
		t.Errorf("OnFail not called")
	}
}
