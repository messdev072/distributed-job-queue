package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisRateLimiter implements rate limiting using Redis token bucket algorithm.
type RedisRateLimiter struct {
	client           *redis.Client
	config           queue.RateLimitConfig
	tokenBucketScript *redis.Script
}

// NewRedisRateLimiter creates a new Redis-based rate limiter.
func NewRedisRateLimiter(client *redis.Client, config queue.RateLimitConfig) *RedisRateLimiter {
	// Lua script for atomic token bucket operations
	tokenBucketScript := redis.NewScript(`
		local key = KEYS[1]
		local rate_per_sec = tonumber(ARGV[1])
		local burst_size = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])
		local tokens_requested = tonumber(ARGV[4])
		
		-- Get current state (tokens and last refill timestamp)
		local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
		local tokens = tonumber(bucket[1]) or burst_size
		local last_refill = tonumber(bucket[2]) or now
		
		-- Calculate tokens to add based on time elapsed
		local time_elapsed = math.max(0, now - last_refill)
		local new_tokens = math.min(burst_size, tokens + (time_elapsed * rate_per_sec))
		
		-- Check if we can consume the requested tokens
		if new_tokens >= tokens_requested then
			-- Consume tokens and update state
			new_tokens = new_tokens - tokens_requested
			redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
			redis.call('EXPIRE', key, 3600) -- Expire after 1 hour of inactivity
			return 1 -- Allow
		else
			-- Update state but don't consume tokens
			redis.call('HMSET', key, 'tokens', new_tokens, 'last_refill', now)
			redis.call('EXPIRE', key, 3600)
			return 0 -- Deny
		end
	`)

	return &RedisRateLimiter{
		client:            client,
		config:            config,
		tokenBucketScript: tokenBucketScript,
	}
}

// AllowQueue checks if a job can be dequeued from the specified queue.
func (r *RedisRateLimiter) AllowQueue(ctx context.Context, queueName string) (bool, error) {
	if !r.config.Enabled || r.config.QueueRatePerSecond <= 0 {
		return true, nil
	}

	key := fmt.Sprintf("rate_limit:queue:%s", queueName)
	now := float64(time.Now().UnixNano()) / 1e9 // Convert to seconds with nanosecond precision
	
	result, err := r.tokenBucketScript.Run(ctx, r.client, []string{key}, 
		r.config.QueueRatePerSecond, 
		r.config.QueueBurstSize, 
		now, 
		1, // Request 1 token
	).Result()
	
	if err != nil {
		return false, fmt.Errorf("failed to check queue rate limit: %w", err)
	}
	
	allowed, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected result type from rate limit script: %T", result)
	}
	
	return allowed == 1, nil
}

// AllowWorker checks if the specified worker can process another job.
func (r *RedisRateLimiter) AllowWorker(ctx context.Context, workerID string) (bool, error) {
	if !r.config.Enabled || r.config.WorkerRatePerSecond <= 0 {
		return true, nil
	}

	key := fmt.Sprintf("rate_limit:worker:%s", workerID)
	now := float64(time.Now().UnixNano()) / 1e9 // Convert to seconds with nanosecond precision
	
	result, err := r.tokenBucketScript.Run(ctx, r.client, []string{key}, 
		r.config.WorkerRatePerSecond, 
		r.config.WorkerBurstSize, 
		now, 
		1, // Request 1 token
	).Result()
	
	if err != nil {
		return false, fmt.Errorf("failed to check worker rate limit: %w", err)
	}
	
	allowed, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected result type from rate limit script: %T", result)
	}
	
	return allowed == 1, nil
}

// GetQueueTokens returns the current number of tokens available for a queue (for debugging/monitoring).
func (r *RedisRateLimiter) GetQueueTokens(ctx context.Context, queueName string) (float64, error) {
	key := fmt.Sprintf("rate_limit:queue:%s", queueName)
	
	bucket, err := r.client.HMGet(ctx, key, "tokens", "last_refill").Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get queue tokens: %w", err)
	}
	
	if bucket[0] == nil {
		return float64(r.config.QueueBurstSize), nil // Default to full bucket
	}
	
	tokens, err := strconv.ParseFloat(bucket[0].(string), 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse tokens: %w", err)
	}
	
	// Apply refill if needed
	if bucket[1] != nil {
		lastRefill, err := strconv.ParseFloat(bucket[1].(string), 64)
		if err == nil {
			now := float64(time.Now().UnixNano()) / 1e9
			timeElapsed := now - lastRefill
			if timeElapsed > 0 {
				tokens = min(float64(r.config.QueueBurstSize), tokens+(timeElapsed*r.config.QueueRatePerSecond))
			}
		}
	}
	
	return tokens, nil
}

// GetWorkerTokens returns the current number of tokens available for a worker (for debugging/monitoring).
func (r *RedisRateLimiter) GetWorkerTokens(ctx context.Context, workerID string) (float64, error) {
	key := fmt.Sprintf("rate_limit:worker:%s", workerID)
	
	bucket, err := r.client.HMGet(ctx, key, "tokens", "last_refill").Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get worker tokens: %w", err)
	}
	
	if bucket[0] == nil {
		return float64(r.config.WorkerBurstSize), nil // Default to full bucket
	}
	
	tokens, err := strconv.ParseFloat(bucket[0].(string), 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse tokens: %w", err)
	}
	
	// Apply refill if needed
	if bucket[1] != nil {
		lastRefill, err := strconv.ParseFloat(bucket[1].(string), 64)
		if err == nil {
			now := float64(time.Now().UnixNano()) / 1e9
			timeElapsed := now - lastRefill
			if timeElapsed > 0 {
				tokens = min(float64(r.config.WorkerBurstSize), tokens+(timeElapsed*r.config.WorkerRatePerSecond))
			}
		}
	}
	
	return tokens, nil
}

// Close cleans up resources.
func (r *RedisRateLimiter) Close() error {
	// Redis client cleanup is handled externally
	return nil
}

// Helper function for min operation
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
