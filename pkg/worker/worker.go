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
			_ = w.Q.Fail(job, err.Error())
		} else {
			fmt.Printf("[Worker] Job %s completed\n", job.ID)
			_ = w.Q.Ack(job)
		}
		job.UpdatedAt = time.Now()
		_ = w.Q.UpdateJob(job)

		// Requeue expired jobs
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			for range ticker.C {
				_ = w.Q.RequeueExpired()
			}
		}()
	}
}
