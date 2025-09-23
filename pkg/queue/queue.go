package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Queue interface {
	Enqueue(job *Job) error
	Dequeue() (*Job, error)
	Ack(job *Job) error
	Fail(job *Job, reason string) error
	GetJob(id string) (*Job, error)
	UpdateJob(job *Job) error
	RequeueExpired() error // Requeue jobs that have been running too long
	Client() *redis.Client
	Ctx() context.Context
}
