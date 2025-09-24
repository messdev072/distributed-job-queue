package worker

import (
	"distributed-job-queue/pkg/queue"
	"fmt"
	"time"
)

type Worker struct {
	ID        string
	Queue     queue.Queue
	QueueName string
	Handler   func(job *queue.Job) error
	Registry  *Registry
}

func (w *Worker) Start() {
	// Defaults
	if w.QueueName == "" {
		w.QueueName = "default"
	}
	if w.ID == "" {
		w.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if w.Registry == nil && w.Queue != nil {
		w.Registry = NewRegistryFromQueue(w.Queue, w.ID)
	}
	if w.Registry != nil {
		w.Registry.StartHeartbeat()
	}

	// Periodic maintenance: requeue expired
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			_ = w.Queue.RequeueExpired(w.QueueName)
		}
	}()

	// Main worker loop
	for {
		job, err := w.Queue.Dequeue(w.QueueName)
		if err != nil {
			time.Sleep(time.Second) // nothing to do, backoff
			continue
		}

		// Record ownership: job is being processed by this worker
		_ = w.Queue.Client().HSet(w.Queue.Ctx(), "job:ownership", job.ID, w.ID).Err()

		job.Status = queue.StatusRunning
		job.UpdatedAt = time.Now()
		_ = w.Queue.UpdateJob(job)

		fmt.Printf("[Worker] Processing job %s: %s\n", job.ID, job.Payload)
		if err := w.Handler(job); err != nil {
			fmt.Printf("[Worker] Job %s failed: %v\n", job.ID, err)
			_ = w.Queue.Fail(job, err.Error())
		} else {
			fmt.Printf("[Worker] Job %s completed\n", job.ID)
			_ = w.Queue.Ack(job)
		}
		job.UpdatedAt = time.Now()
		_ = w.Queue.UpdateJob(job)

		// Next loop iteration
	}
}
