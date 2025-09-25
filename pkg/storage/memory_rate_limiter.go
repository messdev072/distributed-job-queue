package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"sync"
	"time"
)

// tokenBucket represents a token bucket for rate limiting.
type tokenBucket struct {
	tokens     float64   // Current token count
	lastRefill time.Time // Last refill timestamp
	rate       float64   // Tokens per second
	capacity   int       // Maximum tokens (burst size)
	mutex      sync.Mutex
}

// tryConsume attempts to consume tokens from the bucket.
func (tb *tokenBucket) tryConsume(tokensRequested float64) bool {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	now := time.Now()
	
	// Refill tokens based on elapsed time
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed > 0 {
		tb.tokens = min(float64(tb.capacity), tb.tokens+(elapsed*tb.rate))
		tb.lastRefill = now
	}

	// Check if we can consume the requested tokens
	if tb.tokens >= tokensRequested {
		tb.tokens -= tokensRequested
		return true
	}
	
	return false
}

// getTokens returns the current token count (for monitoring).
func (tb *tokenBucket) getTokens() float64 {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed > 0 {
		tb.tokens = min(float64(tb.capacity), tb.tokens+(elapsed*tb.rate))
		tb.lastRefill = now
	}

	return tb.tokens
}

// MemoryRateLimiter implements rate limiting using in-memory token buckets.
type MemoryRateLimiter struct {
	config       queue.RateLimitConfig
	queueBuckets map[string]*tokenBucket
	workerBuckets map[string]*tokenBucket
	mutex        sync.RWMutex
}

// NewMemoryRateLimiter creates a new memory-based rate limiter.
func NewMemoryRateLimiter(config queue.RateLimitConfig) *MemoryRateLimiter {
	return &MemoryRateLimiter{
		config:        config,
		queueBuckets:  make(map[string]*tokenBucket),
		workerBuckets: make(map[string]*tokenBucket),
	}
}

// getOrCreateQueueBucket gets or creates a token bucket for a queue.
func (m *MemoryRateLimiter) getOrCreateQueueBucket(queueName string) *tokenBucket {
	m.mutex.RLock()
	bucket, exists := m.queueBuckets[queueName]
	m.mutex.RUnlock()
	
	if exists {
		return bucket
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Double-check after acquiring write lock
	if bucket, exists := m.queueBuckets[queueName]; exists {
		return bucket
	}

	bucket = &tokenBucket{
		tokens:     float64(m.config.QueueBurstSize), // Start with full bucket
		lastRefill: time.Now(),
		rate:       m.config.QueueRatePerSecond,
		capacity:   m.config.QueueBurstSize,
	}
	
	m.queueBuckets[queueName] = bucket
	return bucket
}

// getOrCreateWorkerBucket gets or creates a token bucket for a worker.
func (m *MemoryRateLimiter) getOrCreateWorkerBucket(workerID string) *tokenBucket {
	m.mutex.RLock()
	bucket, exists := m.workerBuckets[workerID]
	m.mutex.RUnlock()
	
	if exists {
		return bucket
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Double-check after acquiring write lock
	if bucket, exists := m.workerBuckets[workerID]; exists {
		return bucket
	}

	bucket = &tokenBucket{
		tokens:     float64(m.config.WorkerBurstSize), // Start with full bucket
		lastRefill: time.Now(),
		rate:       m.config.WorkerRatePerSecond,
		capacity:   m.config.WorkerBurstSize,
	}
	
	m.workerBuckets[workerID] = bucket
	return bucket
}

// AllowQueue checks if a job can be dequeued from the specified queue.
func (m *MemoryRateLimiter) AllowQueue(ctx context.Context, queueName string) (bool, error) {
	if !m.config.Enabled || m.config.QueueRatePerSecond <= 0 {
		return true, nil
	}

	bucket := m.getOrCreateQueueBucket(queueName)
	return bucket.tryConsume(1.0), nil
}

// AllowWorker checks if the specified worker can process another job.
func (m *MemoryRateLimiter) AllowWorker(ctx context.Context, workerID string) (bool, error) {
	if !m.config.Enabled || m.config.WorkerRatePerSecond <= 0 {
		return true, nil
	}

	bucket := m.getOrCreateWorkerBucket(workerID)
	return bucket.tryConsume(1.0), nil
}

// GetQueueTokens returns the current number of tokens available for a queue (for debugging/monitoring).
func (m *MemoryRateLimiter) GetQueueTokens(ctx context.Context, queueName string) (float64, error) {
	if !m.config.Enabled {
		return float64(m.config.QueueBurstSize), nil
	}

	bucket := m.getOrCreateQueueBucket(queueName)
	return bucket.getTokens(), nil
}

// GetWorkerTokens returns the current number of tokens available for a worker (for debugging/monitoring).
func (m *MemoryRateLimiter) GetWorkerTokens(ctx context.Context, workerID string) (float64, error) {
	if !m.config.Enabled {
		return float64(m.config.WorkerBurstSize), nil
	}

	bucket := m.getOrCreateWorkerBucket(workerID)
	return bucket.getTokens(), nil
}

// Close cleans up resources.
func (m *MemoryRateLimiter) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.queueBuckets = make(map[string]*tokenBucket)
	m.workerBuckets = make(map[string]*tokenBucket)
	return nil
}
