package worker

import (
	"distributed-job-queue/pkg/queue"
	"fmt"
	"time"
)

type Worker struct {
	Q       queue.Queue
	Handler func(job *queue.Job) error
}

func (w *Worker) Start() {
	for {
		job, err := w.Q.Dequeue()
		if err != nil {
			time.Sleep(time.Second) // nothing to do, backoff
			continue
		}

		job.Status = queue.StatusRunning
		job.UpdatedAt = time.Now()
		_ = w.Q.UpdateJob(job)

		fmt.Printf("[Worker] Processing job %s: %s\n", job.ID, job.Payload)
		if err := w.Handler(job); err != nil {
			fmt.Printf("[Worker] Job %s failed: %v\n", job.ID, err)
			job.Status = queue.StatusFailed
		} else {
			fmt.Printf("[Worker] Job %s completed successfully\n", job.ID)
			job.Status = queue.StatusCompleted
		}
		job.UpdatedAt = time.Now()
		_ = w.Q.UpdateJob(job)
	}
}
