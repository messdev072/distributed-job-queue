package worker

import (
	"distributed-job-queue/pkg/logging"
	m "distributed-job-queue/pkg/metrics"
	"distributed-job-queue/pkg/queue"
	"time"

	"go.uber.org/zap"
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
		w.ID = time.Now().Format("20060102150405.000000000")
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

		logging.L().Info("processing job",
			zap.String("job_id", job.ID),
			zap.String("queue", w.QueueName),
			zap.String("worker_id", w.ID),
		)
		if err := w.Handler(job); err != nil {
			logging.L().Error("job failed",
				zap.String("job_id", job.ID),
				zap.String("queue", w.QueueName),
				zap.String("worker_id", w.ID),
				zap.Error(err),
			)
			_ = w.Queue.Fail(job, err.Error())
			m.JobsProcessedTotal.WithLabelValues("fail", w.QueueName).Inc()
			m.ObserveJobCompletion(w.QueueName, job.CreatedAt)
		} else {
			logging.L().Info("job completed",
				zap.String("job_id", job.ID),
				zap.String("queue", w.QueueName),
				zap.String("worker_id", w.ID),
			)
			_ = w.Queue.Ack(job)
			m.JobsProcessedTotal.WithLabelValues("success", w.QueueName).Inc()
			m.ObserveJobCompletion(w.QueueName, job.CreatedAt)
		}
		job.UpdatedAt = time.Now()
		_ = w.Queue.UpdateJob(job)

		// Next loop iteration
	}
}
