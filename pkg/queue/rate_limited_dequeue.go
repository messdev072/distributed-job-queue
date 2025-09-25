package queue

import (
	"context"
	"time"
)

// RateLimitedDequeue wraps a Backend to add rate limiting to dequeue operations.
type RateLimitedDequeue struct {
	backend     Backend
	rateLimiter RateLimiter
}

// Backend interface needs to be imported, but since it causes circular import,
// we'll define a minimal interface here
type Backend interface {
	Dequeue(queueName string, workerID string, visibility time.Duration) (*Job, error)
}

// NewRateLimitedDequeue creates a wrapper that enforces rate limits on dequeue operations.
func NewRateLimitedDequeue(backend Backend, rateLimiter RateLimiter) *RateLimitedDequeue {
	return &RateLimitedDequeue{
		backend:     backend,
		rateLimiter: rateLimiter,
	}
}

// DequeueWithRateLimit attempts to dequeue a job while respecting rate limits.
// Returns nil job if rate limited (no error).
func (r *RateLimitedDequeue) DequeueWithRateLimit(ctx context.Context, queueName, workerID string, visibility time.Duration) (*Job, error) {
	// Check queue rate limit
	allowed, err := r.rateLimiter.AllowQueue(ctx, queueName)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, nil // Rate limited - return nil job, no error
	}

	// Check worker rate limit
	allowed, err = r.rateLimiter.AllowWorker(ctx, workerID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, nil // Rate limited - return nil job, no error
	}

	// Both limits passed, attempt to dequeue
	return r.backend.Dequeue(queueName, workerID, visibility)
}
