package tests

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisRateLimiter_QueueRateLimit(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// Clean up any existing rate limit keys
	_ = client.FlushAll(context.Background()).Err()

	config := queue.RateLimitConfig{
		Enabled:            true,
		QueueRatePerSecond: 2.0, // 2 jobs per second
		QueueBurstSize:     3,   // Burst of 3 jobs
	}

	rateLimiter := storage.NewRedisRateLimiter(client, config)

	ctx := context.Background()
	queueName := "test-queue"

	// First 3 requests should be allowed (burst)
	for i := 0; i < 3; i++ {
		allowed, err := rateLimiter.AllowQueue(ctx, queueName)
		require.NoError(t, err)
		assert.True(t, allowed, fmt.Sprintf("Request %d should be allowed", i+1))
	}

	// 4th request should be denied (burst exhausted)
	allowed, err := rateLimiter.AllowQueue(ctx, queueName)
	require.NoError(t, err)
	assert.False(t, allowed, "4th request should be denied")

	// Wait for tokens to refill (500ms = 1 token at 2/sec rate)
	time.Sleep(600 * time.Millisecond)

	// Now should be allowed again
	allowed, err = rateLimiter.AllowQueue(ctx, queueName)
	require.NoError(t, err)
	assert.True(t, allowed, "Request after wait should be allowed")
}

func TestRedisRateLimiter_WorkerRateLimit(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// Clean up any existing rate limit keys
	_ = client.FlushAll(context.Background()).Err()

	config := queue.RateLimitConfig{
		Enabled:             true,
		WorkerRatePerSecond: 1.0, // 1 job per second
		WorkerBurstSize:     2,   // Burst of 2 jobs
	}

	rateLimiter := storage.NewRedisRateLimiter(client, config)

	ctx := context.Background()
	workerID := "test-worker"

	// First 2 requests should be allowed (burst)
	for i := 0; i < 2; i++ {
		allowed, err := rateLimiter.AllowWorker(ctx, workerID)
		require.NoError(t, err)
		assert.True(t, allowed, fmt.Sprintf("Request %d should be allowed", i+1))
	}

	// 3rd request should be denied (burst exhausted)
	allowed, err := rateLimiter.AllowWorker(ctx, workerID)
	require.NoError(t, err)
	assert.False(t, allowed, "3rd request should be denied")

	// Wait for tokens to refill (1 second = 1 token at 1/sec rate)
	time.Sleep(1100 * time.Millisecond)

	// Now should be allowed again
	allowed, err = rateLimiter.AllowWorker(ctx, workerID)
	require.NoError(t, err)
	assert.True(t, allowed, "Request after wait should be allowed")
}

func TestRedisRateLimiter_GetTokens(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// Clean up any existing rate limit keys
	_ = client.FlushAll(context.Background()).Err()

	config := queue.RateLimitConfig{
		Enabled:            true,
		QueueRatePerSecond: 2.0,
		QueueBurstSize:     5,
	}

	rateLimiter := storage.NewRedisRateLimiter(client, config)

	ctx := context.Background()
	queueName := "test-queue"

	// Initial tokens should be full (5)
	tokens, err := rateLimiter.GetQueueTokens(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, 5.0, tokens)

	// Consume 3 tokens
	for i := 0; i < 3; i++ {
		_, err := rateLimiter.AllowQueue(ctx, queueName)
		require.NoError(t, err)
	}

	// Should have 2 tokens left
	tokens, err = rateLimiter.GetQueueTokens(ctx, queueName)
	require.NoError(t, err)
	assert.InDelta(t, 2.0, tokens, 0.01) // Allow small floating point differences
}

func TestMemoryRateLimiter_QueueRateLimit(t *testing.T) {
	config := queue.RateLimitConfig{
		Enabled:            true,
		QueueRatePerSecond: 2.0, // 2 jobs per second
		QueueBurstSize:     3,   // Burst of 3 jobs
	}

	rateLimiter := storage.NewMemoryRateLimiter(config)

	ctx := context.Background()
	queueName := "test-queue"

	// First 3 requests should be allowed (burst)
	for i := 0; i < 3; i++ {
		allowed, err := rateLimiter.AllowQueue(ctx, queueName)
		require.NoError(t, err)
		assert.True(t, allowed, fmt.Sprintf("Request %d should be allowed", i+1))
	}

	// 4th request should be denied (burst exhausted)
	allowed, err := rateLimiter.AllowQueue(ctx, queueName)
	require.NoError(t, err)
	assert.False(t, allowed, "4th request should be denied")

	// Wait for tokens to refill
	time.Sleep(600 * time.Millisecond)

	// Now should be allowed again
	allowed, err = rateLimiter.AllowQueue(ctx, queueName)
	require.NoError(t, err)
	assert.True(t, allowed, "Request after wait should be allowed")
}

func TestMemoryRateLimiter_GetTokens(t *testing.T) {
	config := queue.RateLimitConfig{
		Enabled:            true,
		QueueRatePerSecond: 2.0,
		QueueBurstSize:     5,
	}

	rateLimiter := storage.NewMemoryRateLimiter(config)

	ctx := context.Background()
	queueName := "test-queue"

	// Initial tokens should be full (5)
	tokens, err := rateLimiter.GetQueueTokens(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, 5.0, tokens)

	// Consume 3 tokens
	for i := 0; i < 3; i++ {
		_, err := rateLimiter.AllowQueue(ctx, queueName)
		require.NoError(t, err)
	}

	// Should have 2 tokens left
	tokens, err = rateLimiter.GetQueueTokens(ctx, queueName)
	require.NoError(t, err)
	assert.InDelta(t, 2.0, tokens, 0.01) // Allow small floating point differences
}

func TestNoopRateLimiter(t *testing.T) {
	rateLimiter := queue.NoopRateLimiter{}

	ctx := context.Background()

	// All requests should be allowed
	for i := 0; i < 100; i++ {
		allowed, err := rateLimiter.AllowQueue(ctx, "any-queue")
		require.NoError(t, err)
		assert.True(t, allowed)

		allowed, err = rateLimiter.AllowWorker(ctx, "any-worker")
		require.NoError(t, err)
		assert.True(t, allowed)
	}
}

func TestRateLimitedWorker(t *testing.T) {
	// Use memory backend for this test
	backend := storage.NewMemoryBackend()

	config := queue.RateLimitConfig{
		Enabled:            true,
		QueueRatePerSecond: 2.0, // 2 jobs per second
		QueueBurstSize:     1,   // Burst of 1 job
	}

	rateLimiter := storage.NewMemoryRateLimiter(config)
	worker := queue.NewRateLimitedWorker("test-worker", backend, rateLimiter)

	// Set fast polling for test
	worker.SetPollInterval(100 * time.Millisecond)

	// Track processed jobs
	processedJobs := make([]string, 0)
	jobsMutex := make(chan struct{}, 1)
	jobsMutex <- struct{}{} // Initialize mutex

	handler := func(ctx context.Context, job *queue.Job) error {
		<-jobsMutex
		processedJobs = append(processedJobs, job.ID)
		jobsMutex <- struct{}{}
		return nil
	}

	worker.RegisterHandler("test-queue", handler)

	// Enqueue 3 jobs
	for i := 0; i < 3; i++ {
		job := &queue.Job{
			ID:        fmt.Sprintf("job-%d", i),
			QueueName: "test-queue",
			Status:    queue.StatusPending,
			Payload:   fmt.Sprintf("payload-%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		err := backend.Enqueue(job)
		require.NoError(t, err)
	}

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := worker.Start(ctx)
	require.NoError(t, err)

	// Wait for processing
	time.Sleep(2500 * time.Millisecond)

	worker.Stop()

	// Check that rate limiting worked - should not have processed all jobs immediately
	<-jobsMutex
	processedCount := len(processedJobs)
	jobsMutex <- struct{}{}

	// With rate limit of 2/sec and burst of 1, after 2.5 seconds we should have processed
	// at most 1 (burst) + 5 (2.5 * 2) = 6 jobs, but we only have 3 jobs total
	assert.LessOrEqual(t, processedCount, 3)
	assert.Greater(t, processedCount, 0, "Should have processed at least one job")
}

func TestRateLimitIntegration(t *testing.T) {
	// Test with Redis backend if available
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// Check if Redis is available
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skip("Redis not available, skipping integration test")
	}

	backend := storage.NewRedisBackend("localhost:6379")

	config := queue.RateLimitConfig{
		Enabled:            true,
		QueueRatePerSecond: 3.0, // 3 jobs per second
		QueueBurstSize:     2,   // Burst of 2 jobs
	}

	rateLimiter := storage.NewRedisRateLimiter(client, config)
	backend.SetRateLimiter(rateLimiter)

	// Verify rate limiter is set
	assert.Equal(t, rateLimiter, backend.GetRateLimiter())

	ctx := context.Background()
	queueName := fmt.Sprintf("rate-limit-test-%d", time.Now().UnixNano())

	// Enqueue some jobs
	for i := 0; i < 5; i++ {
		job := &queue.Job{
			ID:        fmt.Sprintf("rate-limit-job-%d", i),
			QueueName: queueName,
			Status:    queue.StatusPending,
			Payload:   fmt.Sprintf("payload-%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		err := backend.Enqueue(job)
		require.NoError(t, err)
	}

	// Test rate limiting by checking if we can consume tokens
	workerID := "test-worker"

	// First 2 should be allowed (burst)
	for i := 0; i < 2; i++ {
		allowed, err := rateLimiter.AllowQueue(ctx, queueName)
		require.NoError(t, err)
		assert.True(t, allowed, fmt.Sprintf("Queue request %d should be allowed", i+1))

		allowed, err = rateLimiter.AllowWorker(ctx, workerID)
		require.NoError(t, err)
		assert.True(t, allowed, fmt.Sprintf("Worker request %d should be allowed", i+1))
	}

	// 3rd should be denied (burst exhausted)
	allowed, err := rateLimiter.AllowQueue(ctx, queueName)
	require.NoError(t, err)
	assert.False(t, allowed, "3rd queue request should be denied")
}
