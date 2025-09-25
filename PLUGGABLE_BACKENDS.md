# Pluggable Backends Implementation

## Overview
Successfully implemented a pluggable backend system for the distributed job queue, allowing users to choose between different storage implementations while maintaining identical functionality.

## Architecture

### Backend Interface
- **Location**: `pkg/storage/backend.go`
- **Purpose**: Defines the contract for all storage implementations
- **Key Methods**:
  - Core operations: `Enqueue`, `Dequeue`, `Ack`, `Fail`
  - Extended features: `EnqueueWithOpts` (deduplication, idempotency)
  - Maintenance: `RequeueExpired`, `PromoteDelayed`
  - Admin helpers: `ListQueues`, `GetQueueLength`
  - Legacy support: `Client()`, `Ctx()` for Redis-specific operations

### Implemented Backends

#### 1. RedisBackend
- **Location**: `pkg/storage/redis.go`
- **Features**: Full-featured production backend
- **Data Model**:
  - Priority queues using Redis ZSET with score encoding
  - Processing visibility timeouts
  - Delayed job scheduling
  - Dead letter queues
  - Job ownership tracking
  - Deduplication and idempotency
- **Persistence**: All job data stored in Redis hashes as JSON
- **Scalability**: Supports multiple workers and queues

#### 2. MemoryBackend
- **Location**: `pkg/storage/memory.go`
- **Features**: In-memory implementation for testing/development
- **Data Model**: Go maps and slices with proper sorting
- **Thread Safety**: Protected by sync.RWMutex
- **Identical Behavior**: Maintains same semantics as Redis backend

### Compatibility Layer
- **RedisQueue Wrapper**: Maintains backward compatibility with existing code
- **Constructor Functions**:
  - `NewRedisQueue(addr, queue)` - Creates Redis-backed queue (legacy)
  - `NewQueueWithBackend(backend)` - Uses any backend implementation

## Server Configuration

### Environment Variables
- `BACKEND=memory` - Use in-memory backend for testing
- `BACKEND=redis` (default) - Use Redis backend for production
- `REDIS_ADDR` - Redis connection string (default: "localhost:6379")

### Conditional Features
- **Cron Scheduler**: Only available with Redis backend
- **Postgres Hooks**: Compatible with all backends
- **Admin API**: Works with all backends
- **Metrics**: Works with all backends

## Key Features Implemented

### 1. Delivery Semantics
- **At-least-once** (default): Jobs retry on failure
- **At-most-once**: Jobs never retry, removed from processing immediately

### 2. Dead Worker Recovery
- Automatically detects dead workers (missing heartbeats)
- Requeues jobs from dead workers (respects delivery semantics)
- Prevents job loss due to worker crashes

### 3. Priority Queues with FIFO
- Higher priority jobs processed first
- Within same priority: First-In-First-Out ordering
- Score encoding: `-priority*1e12 + timestamp` for stable sorting

### 4. Delayed/Scheduled Jobs
- Jobs can be scheduled for future execution
- Automatic promotion when jobs become ready
- Background ticker promotes delayed jobs every second

### 5. Deduplication & Idempotency
- Prevent duplicate job creation with deduplication keys
- Store and retrieve idempotency results
- Configurable TTL for deduplication and idempotency caches

## Testing

### Test Coverage
- **Backend-specific tests**: `memory_backend_test.go`, existing Redis tests
- **Cross-backend compatibility**: `pluggable_backends_test.go`
- **Integration tests**: All existing tests pass with pluggable system
- **Edge cases**: At-most-once delivery, dead worker recovery, priority ordering

### Test Results
```
=== RUN   TestPluggableBackends
=== RUN   TestPluggableBackends/Redis
=== RUN   TestPluggableBackends/Memory
--- PASS: TestPluggableBackends (0.00s)
    --- PASS: TestPluggableBackends/Redis (0.00s)
    --- PASS: TestPluggableBackends/Memory (0.00s)
PASS
```

All 19 tests pass including the previously failing `TestAtMostOnceNoRetry`.

## Usage Examples

### Using Redis Backend (Production)
```go
backend := storage.NewRedisBackend("localhost:6379")
queue := storage.NewQueueWithBackend(backend)
```

### Using Memory Backend (Testing)
```go
backend := storage.NewMemoryBackend()
queue := storage.NewQueueWithBackend(backend)
```

### Server Startup
```bash
# Production with Redis
./server

# Testing with Memory
BACKEND=memory ./server
```

## Future Extensions

The pluggable architecture makes it easy to add new backends:

1. **PostgresBackend**: Using "SELECT FOR UPDATE SKIP LOCKED"
2. **S3Backend**: For batch processing workloads
3. **DynamoDBBackend**: For AWS-native deployments
4. **FileSystemBackend**: For local development

Each new backend only needs to implement the `Backend` interface to gain full compatibility with all existing features.

## Migration Path

Existing code using `storage.NewRedisQueue()` continues to work unchanged. New code can use `storage.NewQueueWithBackend()` for backend flexibility. The server automatically detects and configures the appropriate backend based on environment variables.
