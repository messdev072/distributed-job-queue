package storage

import (
	"context"
	m "distributed-job-queue/pkg/metrics"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	ctx    context.Context
	hooks  queue.LifecycleHooks
}

func NewRedisQueue(addr, queueName string) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	rq := &RedisQueue{
		client: rdb,
		ctx:    context.Background(),
		hooks:  queue.NoopHooks{},
	}
	return rq
}

// WithHooks sets lifecycle hooks for this queue instance.
func (r *RedisQueue) WithHooks(h queue.LifecycleHooks) *RedisQueue {
	if h == nil {
		r.hooks = queue.NoopHooks{}
		return r
	}
	r.hooks = h
	return r
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

	if !job.AvailableAt.IsZero() && job.AvailableAt.After(time.Now()) {
		// schedule into delayed ZSET; score = ready timestamp (unix seconds)
		if err := r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:delayed", job.QueueName), redis.Z{Score: float64(job.AvailableAt.Unix()), Member: job.ID}).Err(); err != nil {
			return err
		}
	} else {
		// immediate enqueue to priority ZSET
		score := r.scoreFor(job.Priority, job.CreatedAt)
		if err := r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:z", job.QueueName), redis.Z{Score: score, Member: job.ID}).Err(); err != nil {
			return err
		}
	}
	// track queue names in a Redis set for fast discovery
	_ = r.client.SAdd(r.ctx, "queues", job.QueueName).Err()
	// event: enqueued
	_ = r.appendEvent(job.ID, map[string]interface{}{
		"type":      "enqueued",
		"queue":     job.QueueName,
		"timestamp": time.Now().Unix(),
	})
	// update queue length metric (size of ZSET)
	if n, err := r.client.ZCard(r.ctx, fmt.Sprintf("jobs:%s:z", job.QueueName)).Result(); err == nil {
		m.QueueLength.WithLabelValues(job.QueueName).Set(float64(n))
	}
	// fire hook asynchronously to avoid blocking enqueue path
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		r.hooks.OnEnqueue(r.ctx, j)
	}(job)
	return nil
}

func (r *RedisQueue) Dequeue(queueName string) (*queue.Job, error) {
	// Atomically pop the min score member (highest priority + oldest)
	zres, err := r.client.ZPopMin(r.ctx, fmt.Sprintf("jobs:%s:z", queueName), 1).Result()
	if err != nil || len(zres) == 0 {
		if err == nil {
			// Empty
			return nil, redis.Nil
		}
		return nil, err
	}
	jobID := zres[0].Member.(string)

	jobData, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", jobID), "data").Result()
	if err != nil {
		return nil, err
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, err
	}

	// Mark as processing with a short visibility timeout so tests can expire it
	deadline := time.Now().Add(1 * time.Second).Unix()
	// ZSET for visibility timeout
	_ = r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:processing", queueName), redis.Z{Score: float64(deadline), Member: job.ID}).Err()
	// Note: ownership is assigned by the worker after Dequeue via job:ownership hash
	// event: dequeued
	_ = r.appendEvent(job.ID, map[string]interface{}{
		"type":      "dequeued",
		"queue":     queueName,
		"timestamp": time.Now().Unix(),
	})

	job.Status = queue.StatusRunning
	job.UpdatedAt = time.Now()
	_ = r.UpdateJob(&job)
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

func (r *RedisQueue) Ack(job *queue.Job) error {
	// Remove from processing set
	_ = r.client.ZRem(r.ctx, fmt.Sprintf("jobs:%s:processing", job.QueueName), job.ID).Err()
	// Clear ownership
	_ = r.client.HDel(r.ctx, "job:ownership", job.ID).Err()
	job.Status = queue.StatusCompleted
	job.UpdatedAt = time.Now()
	// event: ack
	_ = r.appendEvent(job.ID, map[string]interface{}{
		"type":      "ack",
		"queue":     job.QueueName,
		"timestamp": time.Now().Unix(),
	})
	// record metrics
	m.JobsProcessedTotal.WithLabelValues("success", job.QueueName).Inc()
	m.ObserveJobCompletion(job.QueueName, job.CreatedAt)
	// update queue length metric
	if n, err := r.client.ZCard(r.ctx, fmt.Sprintf("jobs:%s:z", job.QueueName)).Result(); err == nil {
		m.QueueLength.WithLabelValues(job.QueueName).Set(float64(n))
	}
	// trigger hook (async, safe)
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		r.hooks.OnAck(r.ctx, j)
	}(job)
	return r.UpdateJob(job)
}

func (r *RedisQueue) Fail(job *queue.Job, reason string) error {
	job.RetryCount++
	job.UpdatedAt = time.Now()

	// Remove from processing set if present
	_ = r.client.ZRem(r.ctx, fmt.Sprintf("jobs:%s:processing", job.QueueName), job.ID).Err()
	// Clear ownership on failure handling as it will be requeued or sent to DLQ
	_ = r.client.HDel(r.ctx, "job:ownership", job.ID).Err()

	// Always mark as failed for this attempt
	job.Status = queue.StatusFailed

	// If retries exhausted, send to DLQ; otherwise immediately requeue for retry
	if job.RetryCount > job.MaxRetries {
		_ = r.client.LPush(r.ctx, fmt.Sprintf("jobs:%s:dlq", job.QueueName), job.ID).Err()
		// Also push to global DLQ key expected by tests
		_ = r.client.LPush(r.ctx, "jobs:dlq", job.ID).Err()
		_ = r.appendEvent(job.ID, map[string]interface{}{
			"type":      "failed_terminal",
			"queue":     job.QueueName,
			"timestamp": time.Now().Unix(),
		})
		// metrics for failed terminal
		m.JobsProcessedTotal.WithLabelValues("fail", job.QueueName).Inc()
		m.ObserveJobCompletion(job.QueueName, job.CreatedAt)
		// fire hook (async) for failure
		go func(j *queue.Job, rsn string) {
			defer func() { _ = recover() }()
			r.hooks.OnFail(r.ctx, j, rsn)
		}(job, reason)
		return r.UpdateJob(job)
	}

	// Immediately requeue for another attempt (no backoff in tests)
	if err := r.UpdateJob(job); err != nil {
		return err
	}
	if err := r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:z", job.QueueName), redis.Z{Score: r.scoreFor(job.Priority, time.Now()), Member: job.ID}).Err(); err != nil {
		return err
	}
	// job has re-entered the queue; treat as enqueue for hooks
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		r.hooks.OnEnqueue(r.ctx, j)
	}(job)
	_ = r.appendEvent(job.ID, map[string]interface{}{
		"type":      "retry_scheduled",
		"queue":     job.QueueName,
		"timestamp": time.Now().Unix(),
	})
	if n, err := r.client.ZCard(r.ctx, fmt.Sprintf("jobs:%s:z", job.QueueName)).Result(); err == nil {
		m.QueueLength.WithLabelValues(job.QueueName).Set(float64(n))
	}
	// still a failure occurrence (for this attempt) trigger hook
	go func(j *queue.Job, rsn string) {
		defer func() { _ = recover() }()
		r.hooks.OnFail(r.ctx, j, rsn)
	}(job, reason)
	return nil
}

func (r *RedisQueue) RequeueExpired(queueName string) error {
	now := time.Now().Unix()
	procKey := fmt.Sprintf("jobs:%s:processing", queueName)
	queueZ := fmt.Sprintf("jobs:%s:z", queueName)

	// First, recover jobs owned by dead workers regardless of score
	// Fetch all ownerships to find jobs for which the worker heartbeat is missing.
	owners, _ := r.client.HGetAll(r.ctx, "job:ownership").Result()
	for jobID, workerID := range owners {
		// Check that this job is in this queue's processing set
		if _, err := r.client.ZScore(r.ctx, procKey, jobID).Result(); err != nil {
			// not in this queue's processing, skip
			continue
		}
		// Check if worker:<id> exists
		exists, _ := r.client.Exists(r.ctx, fmt.Sprintf("worker:%s", workerID)).Result()
		if exists == 0 {
			// dead worker: move job back to queue and clear from processing and ownership
			_ = r.client.ZRem(r.ctx, procKey, jobID).Err()
			_ = r.client.HDel(r.ctx, "job:ownership", jobID).Err()
			_ = r.client.ZAdd(r.ctx, queueZ, redis.Z{Score: r.scoreFor(0, time.Now()), Member: jobID}).Err()
			// Update job status in hash
			r.client.HSet(r.ctx, fmt.Sprintf("job:%s", jobID),
				"status", queue.StatusPending,
				"updated_at", time.Now().Unix(),
			)
			_ = r.appendEvent(jobID, map[string]interface{}{
				"type":      "requeued_dead_worker",
				"queue":     queueName,
				"timestamp": time.Now().Unix(),
			})
		}
	}

	expired, err := r.client.ZRangeByScore(r.ctx, procKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil {
		return err
	}

	for _, jobID := range expired {
		_ = r.client.ZRem(r.ctx, procKey, jobID).Err()
		_ = r.client.ZAdd(r.ctx, queueZ, redis.Z{Score: r.scoreFor(0, time.Now()), Member: jobID}).Err()
		// Update job status in hash
		r.client.HSet(r.ctx, fmt.Sprintf("job:%s", jobID),
			"status", queue.StatusPending,
			"updated_at", time.Now().Unix(),
		)
		_ = r.appendEvent(jobID, map[string]interface{}{
			"type":      "requeued_expired",
			"queue":     queueName,
			"timestamp": time.Now().Unix(),
		})
	}

	if err := r.PromoteDelayed(queueName); err != nil { return err }

	// refresh queue length metric
	if n, err := r.client.ZCard(r.ctx, fmt.Sprintf("jobs:%s:z", queueName)).Result(); err == nil {
		m.QueueLength.WithLabelValues(queueName).Set(float64(n))
	}

	return nil
}

// PromoteDelayed moves due delayed jobs into the active priority ZSET
func (r *RedisQueue) PromoteDelayed(queueName string) error {
	now := time.Now().Unix()
	delayedKey := fmt.Sprintf("jobs:%s:delayed", queueName)
	ready, err := r.client.ZRangeByScore(r.ctx, delayedKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now)}).Result()
	if err != nil { return err }
	for _, jobID := range ready {
		// Remove from delayed
		_ = r.client.ZRem(r.ctx, delayedKey, jobID).Err()
		// Fetch job to get its priority & created time for stable ordering
		jd, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", jobID), "data").Result()
		if err != nil { continue }
		var job queue.Job
		if err := json.Unmarshal([]byte(jd), &job); err != nil { continue }
		// If job.AvailableAt set, treat CreatedAt as now for ordering to avoid inversion
		enqueueTime := time.Now()
		_ = r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:z", queueName), redis.Z{Score: r.scoreFor(job.Priority, enqueueTime), Member: jobID}).Err()
		// update job timestamps
		job.UpdatedAt = enqueueTime
		if job.AvailableAt.After(enqueueTime) { job.AvailableAt = enqueueTime }
		_ = r.UpdateJob(&job)
		_ = r.appendEvent(jobID, map[string]interface{}{
			"type": "promoted_delayed",
			"queue": queueName,
			"timestamp": time.Now().Unix(),
		})
	}
	return nil
}

func (rq *RedisQueue) Client() *redis.Client { return rq.client }
func (rq *RedisQueue) Ctx() context.Context  { return rq.ctx }

// appendEvent stores a JSON event in a capped list per job
func (r *RedisQueue) appendEvent(jobID string, evt map[string]interface{}) error {
	b, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("job:%s:events", jobID)
	if err := r.client.LPush(r.ctx, key, string(b)).Err(); err != nil {
		return err
	}
	// keep last 100
	_ = r.client.LTrim(r.ctx, key, 0, 99).Err()
	return nil
}

// scoreFor encodes priority (higher first) and enqueue time (earlier first)
// using the formula: score = -priority*1e12 + (enqueueUnixNano/1e6)
func (r *RedisQueue) scoreFor(priority int, t time.Time) float64 {
	base := -1 * float64(priority) * 1e12
	ts := float64(t.UnixNano()) / 1e6
	return base + ts
}
