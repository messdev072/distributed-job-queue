package storage

import (
	"context"
	m "distributed-job-queue/pkg/metrics"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBackend implements Backend interface using Redis as storage.
type RedisBackend struct {
	client        *redis.Client
	ctx           context.Context
	hooks         queue.LifecycleHooks
	rateLimiter   queue.RateLimiter
	tenantManager queue.TenantManager
}

func NewRedisBackend(addr string) *RedisBackend {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisBackend{
		client:      rdb,
		ctx:         context.Background(),
		hooks:       queue.NoopHooks{},
		rateLimiter: queue.NoopRateLimiter{},
	}
}

func (r *RedisBackend) SetHooks(hooks queue.LifecycleHooks) {
	if hooks == nil {
		r.hooks = queue.NoopHooks{}
		return
	}
	r.hooks = hooks
}

func (r *RedisBackend) Enqueue(job *queue.Job) error {
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

func (r *RedisBackend) EnqueueWithOpts(job *queue.Job, dedupeKey, idempotencyKey string) error {
	if dedupeKey != "" {
		dk := fmt.Sprintf("dedupe:%s", dedupeKey)
		// Try SETNX to reserve key
		set, err := r.client.SetNX(r.ctx, dk, job.ID, 10*time.Minute).Result()
		if err != nil {
			return err
		}
		if !set {
			// Existing job ID
			existingID, _ := r.client.Get(r.ctx, dk).Result()
			exJob, err := r.GetJob(existingID)
			if err == nil && exJob != nil {
				// copy existing into passed job pointer for API response consistency
				*job = *exJob
				m.DedupeEvents.WithLabelValues("hit").Inc()
				return nil
			}
			// Fallback: proceed creating new if stale
		}
	}
	if dedupeKey != "" {
		m.DedupeEvents.WithLabelValues("new").Inc()
	}
	if idempotencyKey != "" {
		// Store idempotency key reference on job hash for later lookup
		_ = r.client.HSet(r.ctx, fmt.Sprintf("job:%s", job.ID), "idempotency_key", idempotencyKey).Err()
	}
	return r.Enqueue(job)
}

func (r *RedisBackend) Dequeue(queueName string, workerID string, visibility time.Duration) (*queue.Job, error) {
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

	// Mark as processing with visibility timeout
	deadline := time.Now().Add(visibility).Unix()
	// ZSET for visibility timeout
	_ = r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:processing", queueName), redis.Z{Score: float64(deadline), Member: job.ID}).Err()
	// Set ownership
	_ = r.client.HSet(r.ctx, "job:ownership", job.ID, workerID).Err()
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

func (r *RedisBackend) GetJob(id string) (*queue.Job, error) {
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

func (r *RedisBackend) UpdateJob(job *queue.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return r.client.HSet(r.ctx, fmt.Sprintf("job:%s", job.ID), "data", data).Err()
}

func (r *RedisBackend) Ack(job *queue.Job) error {
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
	// Persist idempotency result if key present
	if key, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", job.ID), "idempotency_key").Result(); err == nil && key != "" {
		_ = r.client.Set(r.ctx, fmt.Sprintf("idempotency:%s:result", key), "COMPLETED", 24*time.Hour).Err()
	}
	return r.UpdateJob(job)
}

func (r *RedisBackend) Fail(job *queue.Job, reason string) error {
	job.RetryCount++
	job.UpdatedAt = time.Now()

	// Remove from processing set if present
	_ = r.client.ZRem(r.ctx, fmt.Sprintf("jobs:%s:processing", job.QueueName), job.ID).Err()
	// Clear ownership on failure handling as it will be requeued or sent to DLQ
	_ = r.client.HDel(r.ctx, "job:ownership", job.ID).Err()

	// Always mark as failed for this attempt
	job.Status = queue.StatusFailed

	// For at_most_once delivery, never retry - send to DLQ immediately
	// Otherwise, check if retries exhausted
	if job.Delivery == "at_most_once" || job.RetryCount > job.MaxRetries {
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
		if key, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", job.ID), "idempotency_key").Result(); err == nil && key != "" {
			_ = r.client.Set(r.ctx, fmt.Sprintf("idempotency:%s:result", key), "FAILED", 24*time.Hour).Err()
		}
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

func (r *RedisBackend) RequeueExpired(queueName string) error {
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
			// dead worker: check if job should be requeued based on delivery mode
			jobDataStr, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", jobID), "data").Result()
			if err != nil {
				continue
			}

			var job queue.Job
			if err := json.Unmarshal([]byte(jobDataStr), &job); err != nil {
				continue
			}

			// For at_most_once delivery mode, if job is already failed, don't requeue
			if job.Delivery == "at_most_once" && job.Status == queue.StatusFailed {
				// Just remove from processing and ownership, don't requeue
				_ = r.client.ZRem(r.ctx, procKey, jobID).Err()
				_ = r.client.HDel(r.ctx, "job:ownership", jobID).Err()
				_ = r.appendEvent(jobID, map[string]interface{}{
					"type":      "dead_worker_at_most_once_not_requeued",
					"queue":     queueName,
					"timestamp": time.Now().Unix(),
				})
				continue
			}

			// Normal requeue for dead worker
			_ = r.client.ZRem(r.ctx, procKey, jobID).Err()
			_ = r.client.HDel(r.ctx, "job:ownership", jobID).Err()
			_ = r.client.ZAdd(r.ctx, queueZ, redis.Z{Score: r.scoreFor(0, time.Now()), Member: jobID}).Err()
			// Update job status to pending
			job.Status = queue.StatusPending
			job.UpdatedAt = time.Now()
			_ = r.UpdateJob(&job)
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
		// First check the job details to see if it should be requeued
		jobDataStr, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", jobID), "data").Result()
		if err != nil {
			continue
		}

		var job queue.Job
		if err := json.Unmarshal([]byte(jobDataStr), &job); err != nil {
			continue
		}

		// For at_most_once delivery mode, if job is already failed, don't requeue
		if job.Delivery == "at_most_once" && job.Status == queue.StatusFailed {
			// Just remove from processing, don't requeue
			_ = r.client.ZRem(r.ctx, procKey, jobID).Err()
			_ = r.appendEvent(jobID, map[string]interface{}{
				"type":      "expired_at_most_once_not_requeued",
				"queue":     queueName,
				"timestamp": time.Now().Unix(),
			})
			continue
		}

		_ = r.client.ZRem(r.ctx, procKey, jobID).Err()
		_ = r.client.ZAdd(r.ctx, queueZ, redis.Z{Score: r.scoreFor(0, time.Now()), Member: jobID}).Err()
		// Update job status to pending
		job.Status = queue.StatusPending
		job.UpdatedAt = time.Now()
		_ = r.UpdateJob(&job)
		_ = r.appendEvent(jobID, map[string]interface{}{
			"type":      "requeued_expired",
			"queue":     queueName,
			"timestamp": time.Now().Unix(),
		})
	}

	if err := r.PromoteDelayed(queueName); err != nil {
		return err
	}

	// refresh queue length metric
	if n, err := r.client.ZCard(r.ctx, fmt.Sprintf("jobs:%s:z", queueName)).Result(); err == nil {
		m.QueueLength.WithLabelValues(queueName).Set(float64(n))
	}

	return nil
}

// PromoteDelayed moves due delayed jobs into the active priority ZSET
func (r *RedisBackend) PromoteDelayed(queueName string) error {
	now := time.Now().Unix()
	delayedKey := fmt.Sprintf("jobs:%s:delayed", queueName)
	ready, err := r.client.ZRangeByScore(r.ctx, delayedKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now)}).Result()
	if err != nil {
		return err
	}
	for _, jobID := range ready {
		// Remove from delayed
		_ = r.client.ZRem(r.ctx, delayedKey, jobID).Err()
		// Fetch job to get its priority & created time for stable ordering
		jd, err := r.client.HGet(r.ctx, fmt.Sprintf("job:%s", jobID), "data").Result()
		if err != nil {
			continue
		}
		var job queue.Job
		if err := json.Unmarshal([]byte(jd), &job); err != nil {
			continue
		}
		// If job.AvailableAt set, treat CreatedAt as now for ordering to avoid inversion
		enqueueTime := time.Now()
		_ = r.client.ZAdd(r.ctx, fmt.Sprintf("jobs:%s:z", queueName), redis.Z{Score: r.scoreFor(job.Priority, enqueueTime), Member: jobID}).Err()
		// update job timestamps
		job.UpdatedAt = enqueueTime
		if job.AvailableAt.After(enqueueTime) {
			job.AvailableAt = enqueueTime
		}
		_ = r.UpdateJob(&job)
		_ = r.appendEvent(jobID, map[string]interface{}{
			"type":      "promoted_delayed",
			"queue":     queueName,
			"timestamp": time.Now().Unix(),
		})
	}
	return nil
}

func (r *RedisBackend) ListQueues() ([]string, error) {
	return r.client.SMembers(r.ctx, "queues").Result()
}

func (r *RedisBackend) GetQueueLength(queueName string) (int64, error) {
	return r.client.ZCard(r.ctx, fmt.Sprintf("jobs:%s:z", queueName)).Result()
}

func (r *RedisBackend) Client() *redis.Client { return r.client }
func (r *RedisBackend) Ctx() context.Context  { return r.ctx }

// appendEvent stores a JSON event in a capped list per job
func (r *RedisBackend) appendEvent(jobID string, evt map[string]interface{}) error {
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
func (r *RedisBackend) scoreFor(priority int, t time.Time) float64 {
	base := -1 * float64(priority) * 1e12
	ts := float64(t.UnixNano()) / 1e6
	return base + ts
}

// Legacy RedisQueue wraps RedisBackend for backward compatibility.
type RedisQueue struct {
	backend Backend
}

func NewRedisQueue(addr, queueName string) *RedisQueue {
	backend := NewRedisBackend(addr)
	return &RedisQueue{
		backend: backend,
	}
}

// NewQueueWithBackend creates a RedisQueue using any Backend implementation.
func NewQueueWithBackend(backend Backend) *RedisQueue {
	return &RedisQueue{
		backend: backend,
	}
}

// WithHooks sets lifecycle hooks for this queue instance.
func (r *RedisQueue) WithHooks(h queue.LifecycleHooks) *RedisQueue {
	r.backend.SetHooks(h)
	return r
}

func (r *RedisQueue) Enqueue(job *queue.Job) error {
	return r.backend.Enqueue(job)
}

func (r *RedisQueue) EnqueueWithOpts(job *queue.Job, dedupeKey, idempotencyKey string) error {
	return r.backend.EnqueueWithOpts(job, dedupeKey, idempotencyKey)
}

func (r *RedisQueue) Dequeue(queueName string) (*queue.Job, error) {
	// Use default visibility timeout of 1 second for legacy compatibility
	return r.backend.Dequeue(queueName, "", 1*time.Second)
}

func (r *RedisQueue) GetJob(id string) (*queue.Job, error) {
	return r.backend.GetJob(id)
}

// SetRateLimiter sets the rate limiter for this backend.
func (r *RedisBackend) SetRateLimiter(limiter queue.RateLimiter) {
	if limiter == nil {
		r.rateLimiter = queue.NoopRateLimiter{}
		return
	}
	r.rateLimiter = limiter
}

// GetRateLimiter returns the current rate limiter.
func (r *RedisBackend) GetRateLimiter() queue.RateLimiter {
	return r.rateLimiter
}

// SetTenantManager sets the tenant manager for this backend.
func (r *RedisBackend) SetTenantManager(manager queue.TenantManager) {
	r.tenantManager = manager
}

// GetTenantManager returns the current tenant manager.
func (r *RedisBackend) GetTenantManager() queue.TenantManager {
	return r.tenantManager
}

// EnqueueTenant enqueues a job for a specific tenant
func (r *RedisBackend) EnqueueTenant(job *queue.Job, tenantID string) error {
	// Set tenant ID on job
	job.TenantID = tenantID

	// Check quotas if tenant manager is available
	if r.tenantManager != nil {
		if err := r.tenantManager.CheckQuotas(r.ctx, tenantID, job); err != nil {
			return err
		}
	}

	// Use tenant-specific keyspace
	tenantKeyspace := "tenant:" + tenantID
	
	// Store job metadata with tenant context
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	
	jobKey := fmt.Sprintf("%s:job:%s", tenantKeyspace, job.ID)
	if err := r.client.HSet(r.ctx, jobKey, "data", data).Err(); err != nil {
		return err
	}

	// Use tenant-specific queue key
	queueKey := fmt.Sprintf("%s:jobs:%s:z", tenantKeyspace, job.QueueName)
	
	// Add to sorted set with priority as score
	score := float64(job.Priority)
	if err := r.client.ZAdd(r.ctx, queueKey, redis.Z{
		Score:  score,
		Member: job.ID,
	}).Err(); err != nil {
		return err
	}

	// Update tenant usage
	if r.tenantManager != nil {
		delta := queue.TenantUsageDelta{
			JobsDelta:        1,
			QueueLengthDelta: 1,
		}
		r.tenantManager.UpdateUsage(r.ctx, tenantID, delta)
	}

	// Call hooks
	r.hooks.OnEnqueue(r.ctx, job)

	return nil
}

// DequeueTenant dequeues a job from a specific tenant's queue
func (r *RedisBackend) DequeueTenant(queueName string, tenantID string, workerID string, visibility time.Duration) (*queue.Job, error) {
	tenantKeyspace := "tenant:" + tenantID
	queueKey := fmt.Sprintf("%s:jobs:%s:z", tenantKeyspace, queueName)
	
	// Check if any jobs are available
	results, err := r.client.ZRevRangeWithScores(r.ctx, queueKey, 0, 0).Result()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil // No jobs available
	}

	// Pop the highest priority job
	jobID := results[0].Member.(string)
	
	// Remove from queue atomically
	removed, err := r.client.ZRem(r.ctx, queueKey, jobID).Result()
	if err != nil {
		return nil, err
	}
	if removed == 0 {
		return nil, nil // Job was already taken by another worker
	}

	// Get job data
	jobKey := fmt.Sprintf("%s:job:%s", tenantKeyspace, jobID)
	jobData, err := r.client.HGet(r.ctx, jobKey, "data").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("job %s not found", jobID)
		}
		return nil, err
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, err
	}

	// Update job status
	job.Status = queue.StatusRunning
	job.UpdatedAt = time.Now()

	// Store in processing set with visibility timeout
	processingKey := fmt.Sprintf("%s:processing:%s", tenantKeyspace, queueName)
	expirationTime := time.Now().Add(visibility).Unix()
	
	err = r.client.ZAdd(r.ctx, processingKey, redis.Z{
		Score:  float64(expirationTime),
		Member: jobID,
	}).Err()
	if err != nil {
		return nil, err
	}

	// Update job metadata
	updatedData, _ := json.Marshal(job)
	r.client.HSet(r.ctx, jobKey, "data", updatedData)

	// Update tenant usage
	if r.tenantManager != nil {
		delta := queue.TenantUsageDelta{
			QueueLengthDelta: -1,
		}
		r.tenantManager.UpdateUsage(r.ctx, tenantID, delta)
	}

	// Note: No OnDequeue hook exists, jobs are handled when they're processed

	return &job, nil
}

// ListTenantQueues returns a list of queues for a specific tenant
func (r *RedisBackend) ListTenantQueues(tenantID string) ([]string, error) {
	pattern := fmt.Sprintf("tenant:%s:jobs:*:z", tenantID)
	keys, err := r.client.Keys(r.ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	var queues []string
	for _, key := range keys {
		// Extract queue name from key: tenant:ID:jobs:QUEUE:z
		parts := strings.Split(key, ":")
		if len(parts) >= 4 {
			queueName := parts[3]
			queues = append(queues, queueName)
		}
	}

	return queues, nil
}

// GetTenantQueueLength returns the length of a specific tenant's queue
func (r *RedisBackend) GetTenantQueueLength(queueName string, tenantID string) (int64, error) {
	queueKey := fmt.Sprintf("tenant:%s:jobs:%s:z", tenantID, queueName)
	return r.client.ZCard(r.ctx, queueKey).Result()
}

func (r *RedisQueue) UpdateJob(job *queue.Job) error {
	return r.backend.UpdateJob(job)
}

func (r *RedisQueue) Ack(job *queue.Job) error {
	return r.backend.Ack(job)
}

func (r *RedisQueue) Fail(job *queue.Job, reason string) error {
	return r.backend.Fail(job, reason)
}

func (r *RedisQueue) RequeueExpired(queueName string) error {
	return r.backend.RequeueExpired(queueName)
}

func (r *RedisQueue) PromoteDelayed(queueName string) error {
	return r.backend.PromoteDelayed(queueName)
}

func (r *RedisQueue) Client() *redis.Client {
	return r.backend.Client()
}

func (r *RedisQueue) Ctx() context.Context {
	return r.backend.Ctx()
}
