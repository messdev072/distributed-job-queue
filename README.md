# Distributed Job Queue

A high-performance, distributed job queue system with pluggable backends and advanced scheduling capabilities.

## Features

- **Pluggable Backends**: Support for Redis, PostgreSQL, and in-memory storage
- **Priority Queues**: Jobs are processed by priority (higher numbers first)
- **Delayed Jobs**: Schedule jobs to run at a specific time
- **Retries with Backoff**: Configurable retry logic with exponential backoff
- **Dead Letter Queue (DLQ)**: Failed jobs are moved to DLQ after max retries
- **Delivery Guarantees**: Support for at-least-once and at-most-once delivery
- **Job Deduplication**: Prevent duplicate jobs using deduplication keys
- **Idempotency**: Ensure operations are idempotent using idempotency keys
- **Worker Lifecycle**: Graceful shutdown and job visibility timeouts
- **Atomic Operations**: Thread-safe operations across all backends

## Backends

### Redis Backend
High-performance backend using Redis as the storage layer.

```bash
BACKEND=redis REDIS_URL=redis://localhost:6379 ./server
```

### PostgreSQL Backend
Production-ready backend using PostgreSQL with ACID transactions.

```bash
BACKEND=postgres POSTGRES_DSN="postgres://user:pass@localhost:5432/jobqueue?sslmode=disable" ./server
```

Features:
- Uses `SELECT FOR UPDATE SKIP LOCKED` for atomic job claiming
- Full ACID transaction support
- Connection pooling with pgxpool
- Automatic table creation and migrations
- Proper indexing for performance

### Memory Backend
In-memory backend for development and testing.

```bash
BACKEND=memory ./server
```

## Environment Variables

### Common
- `BACKEND`: Backend type (`redis`, `postgres`, `memory`)
- `PORT`: Server port (default: 8080)

### Redis Backend
- `REDIS_URL`: Redis connection URL (default: `redis://localhost:6379`)

### PostgreSQL Backend
- `POSTGRES_DSN`: PostgreSQL connection string (required)

Example DSN: `postgres://username:password@localhost:5432/database?sslmode=disable`

## Job Structure

```go
type Job struct {
    ID          string            `json:"id"`
    QueueName   string            `json:"queue_name"`
    Status      string            `json:"status"`
    Payload     string            `json:"payload"`
    Priority    int               `json:"priority"`
    MaxRetries  int               `json:"max_retries"`
    RetryCount  int               `json:"retry_count"`
    Delivery    string            `json:"delivery"`
    AvailableAt time.Time         `json:"available_at"`
    CreatedAt   time.Time         `json:"created_at"`
    UpdatedAt   time.Time         `json:"updated_at"`
    Metadata    map[string]string `json:"metadata"`
}
```

## Usage

### Starting the Server

```bash
# With Redis backend
BACKEND=redis REDIS_URL=redis://localhost:6379 ./server

# With PostgreSQL backend
BACKEND=postgres POSTGRES_DSN="postgres://user:pass@localhost/jobqueue" ./server

# With Memory backend (for testing)
BACKEND=memory ./server
```

### API Endpoints

- `POST /jobs` - Enqueue a new job
- `GET /jobs/:id` - Get job details
- `POST /jobs/:id/ack` - Acknowledge job completion
- `POST /jobs/:id/fail` - Mark job as failed
- `GET /queues` - List all queues
- `GET /queues/:name/length` - Get queue length
- `POST /queues/:name/dequeue` - Dequeue a job from queue

## Testing

### Running Tests

```bash
# Run all tests
go test ./tests/...

# Run specific backend tests
go test ./tests/ -run TestMemoryBackend
go test ./tests/ -run TestRedisBackend
go test ./tests/ -run TestPostgresBackend

# Run with PostgreSQL (requires running PostgreSQL instance)
TEST_POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/job_queue_test?sslmode=disable" go test ./tests/ -run TestPostgres
```

### Setting up PostgreSQL for Testing

```bash
# Using Docker
docker run --name postgres-test -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=job_queue_test -p 5432:5432 -d postgres:15

# Or using local PostgreSQL
createdb job_queue_test
```

## Architecture

The system is built with pluggable backends that all implement the same `Backend` interface:

```go
type Backend interface {
    Enqueue(job *Job) error
    EnqueueWithOpts(job *Job, dedupeKey, idempotencyKey string) error
    Dequeue(queueName, workerID string, visibilityTimeout time.Duration) (*Job, error)
    Ack(job *Job) error
    Fail(job *Job, reason string) error
    GetJob(jobID string) (*Job, error)
    UpdateJob(job *Job) error
    RequeueExpired(queueName string) error
    PromoteDelayed(queueName string) error
    ListQueues() ([]string, error)
    GetQueueLength(queueName string) (int64, error)
}
```

This allows for easy switching between storage backends without changing application code.

## Performance Considerations

### PostgreSQL Backend
- Uses connection pooling for optimal database connections
- Employs `SELECT FOR UPDATE SKIP LOCKED` for high-concurrency job claiming
- Proper indexing on frequently queried columns
- JSONB storage for flexible metadata and event data

### Redis Backend
- Uses atomic Redis operations (ZPOPMIN, etc.)
- Pipeline operations where possible
- Efficient data structures (sorted sets for priorities)

### Memory Backend
- Optimized for development/testing
- Thread-safe using Go mutexes
- No persistence (data lost on restart)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License
