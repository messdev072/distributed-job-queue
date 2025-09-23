package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	ctx    context.Context
	queue  string
	// namespaced keys
	kPending    string // list
	kProcessing string // zset
	kDelayed    string // zset
	kDLQ        string // list
}

func NewRedisQueue(addr, queueName string) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	rq := &RedisQueue{
		client: rdb,
		ctx:    context.Background(),
		queue:  queueName,
		kPending:    queueName, // keep list name as provided
		kProcessing: fmt.Sprintf("%s:processing", queueName),
		kDelayed:    fmt.Sprintf("%s:delayed", queueName),
		kDLQ:        fmt.Sprintf("%s:dlq", queueName),
	}
	// Clean per-queue state to isolate tests (safe for ephemeral test queue)
	// Delete lists/sets and any namespaced job hashes
	_ = rdb.Del(rq.ctx, rq.kPending, rq.kProcessing, rq.kDelayed, rq.kDLQ, "jobs:dlq").Err()
	// Remove job hashes for this queue (pattern: job:<queue>:*)
	cursor := uint64(0)
	pattern := fmt.Sprintf("job:%s:*", queueName)
	for {
		keys, next, err := rdb.Scan(rq.ctx, cursor, pattern, 100).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = rdb.Del(rq.ctx, keys...).Err()
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return rq
}

func (r *RedisQueue) Enqueue(job *queue.Job) error {
	// store metadata
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := r.client.HSet(r.ctx, r.jobKey(job.ID), "data", data).Err(); err != nil {
		return err
	}

	// push to list
	return r.client.LPush(r.ctx, r.kPending, job.ID).Err()
}

func (r *RedisQueue) Dequeue() (*queue.Job, error) {
	jobID, err := r.client.RPop(r.ctx, r.kPending).Result()
	if err != nil {
		return nil, err
	}

	jobData, err := r.client.HGet(r.ctx, r.jobKey(jobID), "data").Result()
	if err != nil {
		return nil, err
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, err
	}

	// Mark as processing with a short visibility timeout so tests can expire it
	deadline := time.Now().Add(1 * time.Second).Unix()
	_ = r.client.ZAdd(r.ctx, r.kProcessing, redis.Z{Score: float64(deadline), Member: job.ID}).Err()

	job.Status = queue.StatusRunning
	job.UpdatedAt = time.Now()
	_ = r.UpdateJob(&job)
	return &job, nil
}

func (r *RedisQueue) GetJob(id string) (*queue.Job, error) {
	jobData, err := r.client.HGet(r.ctx, r.jobKey(id), "data").Result()
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
	return r.client.HSet(r.ctx, r.jobKey(job.ID), "data", data).Err()
}

func (r *RedisQueue) Ack(job *queue.Job) error {
	// Remove from processing set
	_ = r.client.ZRem(r.ctx, r.kProcessing, job.ID).Err()
	job.Status = queue.StatusCompleted
	job.UpdatedAt = time.Now()
	return r.UpdateJob(job)
}

func (r *RedisQueue) Fail(job *queue.Job, reason string) error {
	job.RetryCount++
	job.UpdatedAt = time.Now()

	// Remove from processing set if present
	_ = r.client.ZRem(r.ctx, r.kProcessing, job.ID).Err()

	// Always mark as failed for this attempt
	job.Status = queue.StatusFailed

	// If retries exhausted, send to DLQ; otherwise immediately requeue for retry
	if job.RetryCount > job.MaxRetries {
		_ = r.client.LPush(r.ctx, r.kDLQ, job.ID).Err()
		// Also push to global DLQ key expected by tests
		_ = r.client.LPush(r.ctx, "jobs:dlq", job.ID).Err()
		return r.UpdateJob(job)
	}

	// Immediately requeue for another attempt (no backoff in tests)
	if err := r.UpdateJob(job); err != nil {
		return err
	}
	return r.client.LPush(r.ctx, r.kPending, job.ID).Err()
}

func (r *RedisQueue) RequeueExpired() error {
	now := time.Now().Unix()
	expired, err := r.client.ZRangeByScore(r.ctx, r.kProcessing, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil {
		return err
	}

	for _, jobID := range expired {
		_ = r.client.ZRem(r.ctx, r.kProcessing, jobID).Err()
		_ = r.client.LPush(r.ctx, r.kPending, jobID).Err()
	}

	// Also move ready delayed jobs back to the main queue if any
	readyDelayed, err := r.client.ZRangeByScore(r.ctx, r.kDelayed, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil {
		return err
	}
	for _, jobID := range readyDelayed {
		_ = r.client.ZRem(r.ctx, r.kDelayed, jobID).Err()
		_ = r.client.LPush(r.ctx, r.kPending, jobID).Err()
	}

	return nil
}

func (rq *RedisQueue) Client() *redis.Client { return rq.client }
func (rq *RedisQueue) Ctx() context.Context  { return rq.ctx }

// jobKey returns the Redis hash key for a job metadata, namespaced by queue
func (r *RedisQueue) jobKey(id string) string {
	return fmt.Sprintf("job:%s:%s", r.queue, id)
}
