# Rate Limiting Documentation

## Overview

The distributed job queue system includes comprehensive rate limiting capabilities with two main throttling variants:

1. **Per-queue rate limiting**: Global throughput limit per queue
2. **Per-worker throttling**: Each worker limits processing speed

## Implementation

The rate limiting system uses a **token bucket algorithm** with atomic operations to ensure consistency across concurrent workers.

### Supported Backends

- **Redis**: Recommended for production (atomic Lua scripts)
- **Memory**: For testing and single-node deployments

## Configuration

```go
rateLimitConfig := queue.RateLimitConfig{
    Enabled:             true,  // Enable/disable rate limiting
    QueueRatePerSecond:  5.0,   // Jobs per second per queue
    QueueBurstSize:      10,    // Maximum burst size for queues
    WorkerRatePerSecond: 2.0,   // Jobs per second per worker
    WorkerBurstSize:     3,     // Maximum burst size for workers
}
```

### Configuration Parameters

- **QueueRatePerSecond**: Maximum jobs processed per second per queue (float64)
- **QueueBurstSize**: Maximum tokens in the queue bucket (allows bursts) (int)
- **WorkerRatePerSecond**: Maximum jobs processed per second per worker (float64) 
- **WorkerBurstSize**: Maximum tokens in the worker bucket (int)
- **Enabled**: Global enable/disable flag (bool)

## Usage Examples

### Redis Rate Limiter

```go
// Create Redis connection
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

// Create Redis backend
backend := storage.NewRedisBackend("localhost:6379")

// Configure rate limiting
config := queue.RateLimitConfig{
    Enabled:             true,
    QueueRatePerSecond:  5.0,
    QueueBurstSize:      10,
    WorkerRatePerSecond: 2.0,
    WorkerBurstSize:     3,
}

// Create rate limiter
rateLimiter := storage.NewRedisRateLimiter(client, config)
backend.SetRateLimiter(rateLimiter)

// Create rate-limited worker
worker := queue.NewRateLimitedWorker("worker-1", backend, rateLimiter)

// Register handlers
worker.RegisterHandler("high-priority", func(ctx context.Context, job *queue.Job) error {
    // Process job
    return nil
})

// Start worker
ctx := context.Background()
go worker.Start(ctx)
```

### Memory Rate Limiter

```go
// Create memory backend
backend := storage.NewMemoryBackend()

// Configure rate limiting
config := queue.RateLimitConfig{
    Enabled:             true,
    QueueRatePerSecond:  3.0,
    QueueBurstSize:      5,
    WorkerRatePerSecond: 1.0,
    WorkerBurstSize:     2,
}

// Create rate limiter
rateLimiter := storage.NewMemoryRateLimiter(config)
backend.SetRateLimiter(rateLimiter)

// Use with worker
worker := queue.NewRateLimitedWorker("memory-worker", backend, rateLimiter)
```

### Manual Rate Limit Checks

```go
// Check if queue processing is allowed
allowed, err := rateLimiter.AllowQueue(ctx, "my-queue")
if err != nil {
    return err
}

if !allowed {
    // Rate limit exceeded, wait or skip
    return nil
}

// Check if worker processing is allowed
allowed, err = rateLimiter.AllowWorker(ctx, "worker-1")
if err != nil {
    return err
}

if !allowed {
    // Worker rate limit exceeded
    return nil
}
```

### Monitoring Token Levels

```go
// Get current token count for a queue
tokens, err := rateLimiter.GetQueueTokens(ctx, "my-queue")
if err != nil {
    return err
}
fmt.Printf("Queue has %.2f tokens available\n", tokens)

// Get current token count for a worker
tokens, err = rateLimiter.GetWorkerTokens(ctx, "worker-1")
if err != nil {
    return err
}
fmt.Printf("Worker has %.2f tokens available\n", tokens)
```

## Rate Limiting Behavior

### Token Bucket Algorithm

1. **Token Generation**: Tokens are added to buckets at the configured rate
2. **Token Consumption**: Each job processing attempt consumes one token
3. **Burst Handling**: Bucket size allows for burst processing up to the limit
4. **Rate Enforcement**: When no tokens available, processing is blocked/delayed

### Atomic Operations

- **Redis**: Uses Lua scripts for atomic token consumption and refill
- **Memory**: Uses mutex-protected operations for thread safety

### Key Naming Convention (Redis)

- Queue tokens: `rate:queue:<queue-name>`
- Worker tokens: `rate:worker:<worker-id>`

Each key stores:
- `tokens`: Current token count (float)
- `last_refill`: Last refill timestamp (unix timestamp)

## Integration with Workers

The `RateLimitedWorker` automatically:

1. **Checks queue rate limits** before dequeuing jobs
2. **Checks worker rate limits** before processing jobs
3. **Handles rate limit violations** by waiting/skipping
4. **Maintains token buckets** through background processes
5. **Provides concurrent processing** with per-goroutine rate limiting

### Worker Configuration

```go
worker := queue.NewRateLimitedWorker("worker-id", backend, rateLimiter)

// Configure concurrency
worker.SetConcurrency(4) // 4 concurrent goroutines

// Configure polling
worker.SetPollInterval(100 * time.Millisecond)

// Register handlers for different queue types
worker.RegisterHandler("queue1", handler1)
worker.RegisterHandler("queue2", handler2)

// Start processing
ctx := context.Background()
go worker.Start(ctx)
```

## Testing

Run the comprehensive test suite:

```bash
cd distributed-job-queue
go test -v ./tests/rate_limiting_test.go
```

Tests cover:
- Redis rate limiter functionality
- Memory rate limiter functionality  
- Token bucket mechanics
- Rate-limited worker behavior
- Integration scenarios

## Performance Considerations

### Redis Rate Limiter

- **Pros**: Distributed, atomic, persistent
- **Cons**: Network latency for each rate check
- **Best for**: Multi-node deployments, production environments

### Memory Rate Limiter

- **Pros**: Low latency, no network dependency
- **Cons**: Not distributed, lost on restart
- **Best for**: Single-node deployments, testing, development

### Optimization Tips

1. **Batch Operations**: Process multiple jobs per rate limit check when possible
2. **Appropriate Polling**: Balance between responsiveness and rate limit overhead
3. **Burst Configuration**: Set appropriate burst sizes for workload patterns
4. **Monitoring**: Track token levels to optimize rate configurations

## Troubleshooting

### Common Issues

1. **Rate limits too restrictive**: Increase rate per second or burst size
2. **Redis connection issues**: Verify Redis connectivity and permissions
3. **Clock synchronization**: Ensure system clocks are synchronized for distributed setups
4. **Token bucket overflow**: Monitor and adjust burst sizes based on actual workload

### Debugging

Enable debug logging to see rate limiting decisions:

```go
// Add logging to see rate limiting behavior
log.Printf("Queue rate limit check: allowed=%t, tokens=%.2f", allowed, tokens)
```

### Monitoring

Track these metrics:
- Rate limit violations per queue/worker
- Average token levels over time
- Processing throughput vs configured limits
- Queue lengths during rate limiting
