package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	ctx    context.Context
	queue  string
}

func NewRedisQueue(addr, queueName string) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisQueue{
		client: rdb,
		ctx:    context.Background(),
		queue:  queueName,
	}
}

func (r *RedisQueue) Enqueue(job *queue.Job) error {
	// store metadata
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := r.client.HSet(r.ctx, fmt.Sprintf("job:%s", job.ID), "data", data).Err(); err != nil {
		return err
	}

	// push to list
	return r.client.LPush(r.ctx, r.queue, job.ID).Err()
}

func (r *RedisQueue) Dequeue() (*queue.Job, error) {
	jobID, err := r.client.RPop(r.ctx, r.queue).Result()
	if err != nil {
		return nil, err
	}

	jobData, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", jobID), "data").Result()
	if err != nil {
		return nil, err
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *RedisQueue) GetJob(id string) (*queue.Job, error) {
	jobData, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", id), "data").Result()
	if err != nil {
		return nil, err
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *RedisQueue) UpdateJob(job *queue.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return r.client.HSet(r.ctx, fmt.Sprintf("job:%s", job.ID), "data", data).Err()
}
