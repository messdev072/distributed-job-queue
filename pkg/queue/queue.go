package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Queue interface {
	Enqueue(job *Job) error
	EnqueueWithOpts(job *Job, dedupeKey, idempotencyKey string) error
	Dequeue(queueName string) (*Job, error)
	Ack(job *Job) error
	Fail(job *Job, reason string) error
	GetJob(id string) (*Job, error)
	UpdateJob(job *Job) error
	RequeueExpired(queueName string) error // Requeue jobs that have been running too long
	PromoteDelayed(queueName string) error // Promote due delayed jobs
	Client() *redis.Client
	Ctx() context.Context
}
