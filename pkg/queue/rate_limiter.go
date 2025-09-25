package queue

import (
	"context"
)

// RateLimiter defines the interface for rate limiting job processing.
type RateLimiter interface {
	// AllowQueue checks if a job can be dequeued from the specified queue.
	// Returns true if allowed, false if rate limited.
	AllowQueue(ctx context.Context, queueName string) (bool, error)
	
	// AllowWorker checks if the specified worker can process another job.
	// Returns true if allowed, false if rate limited.
	AllowWorker(ctx context.Context, workerID string) (bool, error)
	
	// Close cleans up any resources used by the rate limiter.
	Close() error
}

// RateLimitConfig holds configuration for rate limiting.
type RateLimitConfig struct {
	// QueueRatePerSecond defines the maximum jobs per second per queue
	QueueRatePerSecond float64
	
	// QueueBurstSize defines the maximum burst size for queue rate limiting
	QueueBurstSize int
	
	// WorkerRatePerSecond defines the maximum jobs per second per worker
	WorkerRatePerSecond float64
	
	// WorkerBurstSize defines the maximum burst size for worker rate limiting
	WorkerBurstSize int
	
	// Enabled determines if rate limiting is active
	Enabled bool
}

// NoopRateLimiter is a rate limiter implementation that allows all requests.
type NoopRateLimiter struct{}

// AllowQueue always returns true for the noop rate limiter.
func (n NoopRateLimiter) AllowQueue(ctx context.Context, queueName string) (bool, error) {
	return true, nil
}

// AllowWorker always returns true for the noop rate limiter.
func (n NoopRateLimiter) AllowWorker(ctx context.Context, workerID string) (bool, error) {
	return true, nil
}

// Close is a no-op for the noop rate limiter.
func (n NoopRateLimiter) Close() error {
	return nil
}
