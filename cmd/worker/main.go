package main

import (
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"distributed-job-queue/pkg/worker"
	"errors"
)

func main() {
	q := storage.NewRedisQueue("localhost:6379", "jobs")

	w := &worker.Worker{
		Queue: q,
		Handler: func(job *queue.Job) error {
			// Replace with actual job logic
			if job.Payload == "fail" {
				return errors.New("simulated job failure")
			}
			return nil
		},
	}

	w.Start()
}
