package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// PostgresBackend implements Backend interface using PostgreSQL for job storage.
type PostgresBackend struct {
	pool        *pgxpool.Pool
	hooks       queue.LifecycleHooks
	rateLimiter queue.RateLimiter
}

// NewPostgresBackend creates a new PostgreSQL backend.
func NewPostgresBackend(dsn string) (*PostgresBackend, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	backend := &PostgresBackend{
		pool:        pool,
		hooks:       queue.NoopHooks{},
		rateLimiter: queue.NoopRateLimiter{},
	}

	if err := backend.initTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	return backend, nil
}

// initTables creates the necessary database tables.
func (p *PostgresBackend) initTables() error {
	ctx := context.Background()

	// Main jobs table with all job metadata
	_, err := p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jobs (
			id VARCHAR(255) PRIMARY KEY,
			queue_name VARCHAR(255) NOT NULL,
			status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
			payload TEXT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			delivery VARCHAR(50) NOT NULL DEFAULT 'at_least_once',
			available_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			recurrence_id VARCHAR(255),
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			processing_until TIMESTAMP WITH TIME ZONE,
			worker_id VARCHAR(255)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create jobs table: %w", err)
	}

	// Create indexes for jobs table
	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_queue_status ON jobs (queue_name, status)`)
	if err != nil {
		return fmt.Errorf("failed to create jobs queue status index: %w", err)
	}

	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_queue_priority_created ON jobs (queue_name, status, priority DESC, created_at ASC)`)
	if err != nil {
		return fmt.Errorf("failed to create jobs priority index: %w", err)
	}

	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_available_at ON jobs (available_at)`)
	if err != nil {
		return fmt.Errorf("failed to create jobs available_at index: %w", err)
	}

	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_processing_until ON jobs (processing_until)`)
	if err != nil {
		return fmt.Errorf("failed to create jobs processing_until index: %w", err)
	}

	// Dead letter queue table
	_, err = p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jobs_dlq (
			id VARCHAR(255) PRIMARY KEY,
			queue_name VARCHAR(255) NOT NULL,
			original_job JSONB NOT NULL,
			failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			failure_reason TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create jobs_dlq table: %w", err)
	}

	// Create indexes for jobs_dlq table
	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_dlq_queue ON jobs_dlq (queue_name)`)
	if err != nil {
		return fmt.Errorf("failed to create jobs_dlq queue index: %w", err)
	}

	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_dlq_failed_at ON jobs_dlq (failed_at)`)
	if err != nil {
		return fmt.Errorf("failed to create jobs_dlq failed_at index: %w", err)
	}

	// Deduplication table
	_, err = p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS job_deduplication (
			dedupe_key VARCHAR(255) PRIMARY KEY,
			job_id VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create job_deduplication table: %w", err)
	}

	// Idempotency results table
	_, err = p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS job_idempotency (
			idempotency_key VARCHAR(255) PRIMARY KEY,
			result TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create job_idempotency table: %w", err)
	}

	// Job events table for audit trail
	_, err = p.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS job_events (
			id BIGSERIAL PRIMARY KEY,
			job_id VARCHAR(255) NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			queue_name VARCHAR(255) NOT NULL,
			event_data JSONB,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create job_events table: %w", err)
	}

	// Create indexes for job_events table
	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events (job_id)`)
	if err != nil {
		return fmt.Errorf("failed to create job_events job_id index: %w", err)
	}

	_, err = p.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_job_events_created_at ON job_events (created_at)`)
	if err != nil {
		return fmt.Errorf("failed to create job_events created_at index: %w", err)
	}

	return nil
}

func (p *PostgresBackend) SetHooks(hooks queue.LifecycleHooks) {
	if hooks == nil {
		p.hooks = queue.NoopHooks{}
		return
	}
	p.hooks = hooks
}

func (p *PostgresBackend) Enqueue(job *queue.Job) error {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert job into jobs table
	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (
			id, queue_name, status, payload, priority, delivery, available_at,
			recurrence_id, created_at, updated_at, retry_count, max_retries
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, job.ID, job.QueueName, job.Status, job.Payload, job.Priority, job.Delivery,
		job.AvailableAt, job.RecurrenceID, job.CreatedAt, job.UpdatedAt,
		job.RetryCount, job.MaxRetries)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	// Add event
	if err := p.addEvent(ctx, tx, job.ID, "enqueued", job.QueueName, nil); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Fire hook asynchronously
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		p.hooks.OnEnqueue(context.Background(), j)
	}(job)

	return nil
}

func (p *PostgresBackend) EnqueueWithOpts(job *queue.Job, dedupeKey, idempotencyKey string) error {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Handle deduplication
	if dedupeKey != "" {
		// Clean up expired deduplication entries first
		_, err = tx.Exec(ctx, "DELETE FROM job_deduplication WHERE expires_at < NOW()")
		if err != nil {
			return fmt.Errorf("failed to clean up deduplication entries: %w", err)
		}

		// Check if dedupe key exists
		var existingJobID string
		err = tx.QueryRow(ctx,
			"SELECT job_id FROM job_deduplication WHERE dedupe_key = $1",
			dedupeKey).Scan(&existingJobID)

		if err == nil {
			// Found existing job, load it
			existingJob, err := p.getJobWithTx(ctx, tx, existingJobID)
			if err == nil && existingJob != nil {
				*job = *existingJob
				return tx.Commit(ctx) // Return existing job
			}
		} else if err != pgx.ErrNoRows {
			return fmt.Errorf("failed to check deduplication: %w", err)
		}

		// Insert deduplication entry
		expiresAt := time.Now().Add(10 * time.Minute)
		_, err = tx.Exec(ctx,
			"INSERT INTO job_deduplication (dedupe_key, job_id, expires_at) VALUES ($1, $2, $3)",
			dedupeKey, job.ID, expiresAt)
		if err != nil {
			return fmt.Errorf("failed to insert deduplication entry: %w", err)
		}
	}

	// Insert the job normally
	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (
			id, queue_name, status, payload, priority, delivery, available_at,
			recurrence_id, created_at, updated_at, retry_count, max_retries
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, job.ID, job.QueueName, job.Status, job.Payload, job.Priority, job.Delivery,
		job.AvailableAt, job.RecurrenceID, job.CreatedAt, job.UpdatedAt,
		job.RetryCount, job.MaxRetries)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	// Handle idempotency key storage
	if idempotencyKey != "" {
		expiresAt := time.Now().Add(24 * time.Hour)
		_, err = tx.Exec(ctx,
			"INSERT INTO job_idempotency (idempotency_key, result, expires_at) VALUES ($1, $2, $3) ON CONFLICT (idempotency_key) DO NOTHING",
			idempotencyKey, "PENDING", expiresAt)
		if err != nil {
			return fmt.Errorf("failed to insert idempotency entry: %w", err)
		}
	}

	// Add event
	if err := p.addEvent(ctx, tx, job.ID, "enqueued", job.QueueName, nil); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Fire hook asynchronously
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		p.hooks.OnEnqueue(context.Background(), j)
	}(job)

	return nil
}

func (p *PostgresBackend) Dequeue(queueName string, workerID string, visibility time.Duration) (*queue.Job, error) {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	processingUntil := time.Now().Add(visibility)

	// Use SELECT FOR UPDATE SKIP LOCKED for atomic job claiming
	// This is PostgreSQL's equivalent to Redis ZPOPMIN - atomically gets highest priority job
	var job queue.Job
	err = tx.QueryRow(ctx, `
		UPDATE jobs 
		SET status = 'RUNNING', 
		    processing_until = $1, 
		    worker_id = $2, 
		    updated_at = NOW()
		WHERE id = (
			SELECT id FROM jobs 
			WHERE queue_name = $3 
			  AND status = 'PENDING' 
			  AND (available_at IS NULL OR available_at <= NOW())
			ORDER BY priority DESC, created_at ASC 
			FOR UPDATE SKIP LOCKED 
			LIMIT 1
		)
		RETURNING id, queue_name, status, payload, priority, delivery, available_at,
		          recurrence_id, created_at, updated_at, retry_count, max_retries
	`, processingUntil, workerID, queueName).Scan(
		&job.ID, &job.QueueName, &job.Status, &job.Payload, &job.Priority,
		&job.Delivery, &job.AvailableAt, &job.RecurrenceID,
		&job.CreatedAt, &job.UpdatedAt, &job.RetryCount, &job.MaxRetries)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("no jobs available")
		}
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}

	// Add event
	if err := p.addEvent(ctx, tx, job.ID, "dequeued", queueName, nil); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &job, nil
}

func (p *PostgresBackend) Ack(job *queue.Job) error {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update job status to completed
	job.Status = queue.StatusCompleted
	job.UpdatedAt = time.Now()

	_, err = tx.Exec(ctx, `
		UPDATE jobs 
		SET status = $1, updated_at = $2, processing_until = NULL, worker_id = NULL
		WHERE id = $3
	`, job.Status, job.UpdatedAt, job.ID)
	if err != nil {
		return fmt.Errorf("failed to ack job: %w", err)
	}

	// Add event
	if err := p.addEvent(ctx, tx, job.ID, "ack", job.QueueName, nil); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Fire hook asynchronously
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		p.hooks.OnAck(context.Background(), j)
	}(job)

	return nil
}

func (p *PostgresBackend) Fail(job *queue.Job, reason string) error {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	job.RetryCount++
	job.UpdatedAt = time.Now()
	job.Status = queue.StatusFailed

	// For at_most_once delivery, never retry - send to DLQ immediately
	// Otherwise, check if retries exhausted
	if job.Delivery == "at_most_once" || job.RetryCount > job.MaxRetries {
		// Move to dead letter queue
		jobData, _ := json.Marshal(job)
		_, err = tx.Exec(ctx, `
			INSERT INTO jobs_dlq (id, queue_name, original_job, failure_reason)
			VALUES ($1, $2, $3, $4)
		`, job.ID, job.QueueName, jobData, reason)
		if err != nil {
			return fmt.Errorf("failed to insert into DLQ: %w", err)
		}

		// Remove from main jobs table
		_, err = tx.Exec(ctx, "DELETE FROM jobs WHERE id = $1", job.ID)
		if err != nil {
			return fmt.Errorf("failed to delete job: %w", err)
		}

		// Add terminal failure event
		if err := p.addEvent(ctx, tx, job.ID, "failed_terminal", job.QueueName, map[string]interface{}{
			"reason": reason,
		}); err != nil {
			return err
		}
	} else {
		// Reset for retry
		_, err = tx.Exec(ctx, `
			UPDATE jobs 
			SET status = 'PENDING', retry_count = $1, updated_at = $2, 
			    processing_until = NULL, worker_id = NULL
			WHERE id = $3
		`, job.RetryCount, job.UpdatedAt, job.ID)
		if err != nil {
			return fmt.Errorf("failed to update job for retry: %w", err)
		}

		// Add retry scheduled event
		if err := p.addEvent(ctx, tx, job.ID, "retry_scheduled", job.QueueName, map[string]interface{}{
			"reason":      reason,
			"retry_count": job.RetryCount,
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Fire hook asynchronously
	go func(j *queue.Job, rsn string) {
		defer func() { _ = recover() }()
		p.hooks.OnFail(context.Background(), j, rsn)
	}(job, reason)

	return nil
}

func (p *PostgresBackend) GetJob(id string) (*queue.Job, error) {
	ctx := context.Background()
	return p.getJobWithTx(ctx, nil, id)
}

func (p *PostgresBackend) getJobWithTx(ctx context.Context, tx pgx.Tx, id string) (*queue.Job, error) {
	var job queue.Job
	var query string
	var executor interface {
		QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	}

	query = `
		SELECT id, queue_name, status, payload, priority, delivery, available_at,
		       recurrence_id, created_at, updated_at, retry_count, max_retries
		FROM jobs WHERE id = $1
	`

	if tx != nil {
		executor = tx
	} else {
		executor = p.pool
	}

	err := executor.QueryRow(ctx, query, id).Scan(
		&job.ID, &job.QueueName, &job.Status, &job.Payload, &job.Priority,
		&job.Delivery, &job.AvailableAt, &job.RecurrenceID,
		&job.CreatedAt, &job.UpdatedAt, &job.RetryCount, &job.MaxRetries)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("job not found")
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	return &job, nil
}

func (p *PostgresBackend) UpdateJob(job *queue.Job) error {
	ctx := context.Background()

	_, err := p.pool.Exec(ctx, `
		UPDATE jobs 
		SET status = $1, payload = $2, priority = $3, delivery = $4, available_at = $5,
		    recurrence_id = $6, updated_at = $7, retry_count = $8, max_retries = $9
		WHERE id = $10
	`, job.Status, job.Payload, job.Priority, job.Delivery, job.AvailableAt,
		job.RecurrenceID, job.UpdatedAt, job.RetryCount, job.MaxRetries, job.ID)

	if err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	return nil
}

func (p *PostgresBackend) RequeueExpired(queueName string) error {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Find expired jobs in processing state
	rows, err := tx.Query(ctx, `
		SELECT id, delivery, status
		FROM jobs 
		WHERE queue_name = $1 
		  AND status = 'RUNNING' 
		  AND processing_until < NOW()
	`, queueName)
	if err != nil {
		return fmt.Errorf("failed to query expired jobs: %w", err)
	}
	defer rows.Close()

	var expiredJobs []struct {
		ID       string
		Delivery string
		Status   string
	}

	for rows.Next() {
		var job struct {
			ID       string
			Delivery string
			Status   string
		}
		if err := rows.Scan(&job.ID, &job.Delivery, &job.Status); err != nil {
			return fmt.Errorf("failed to scan expired job: %w", err)
		}
		expiredJobs = append(expiredJobs, job)
	}

	// Process each expired job
	for _, expiredJob := range expiredJobs {
		// For at_most_once delivery mode, if job is already failed, don't requeue
		if expiredJob.Delivery == "at_most_once" && expiredJob.Status == string(queue.StatusFailed) {
			// Just remove from processing
			_, err = tx.Exec(ctx, `
				UPDATE jobs 
				SET processing_until = NULL, worker_id = NULL 
				WHERE id = $1
			`, expiredJob.ID)
			if err != nil {
				return fmt.Errorf("failed to clear processing for at_most_once job: %w", err)
			}

			if err := p.addEvent(ctx, tx, expiredJob.ID, "expired_at_most_once_not_requeued", queueName, nil); err != nil {
				return err
			}
		} else {
			// Normal requeue
			_, err = tx.Exec(ctx, `
				UPDATE jobs 
				SET status = 'PENDING', processing_until = NULL, worker_id = NULL, updated_at = NOW()
				WHERE id = $1
			`, expiredJob.ID)
			if err != nil {
				return fmt.Errorf("failed to requeue expired job: %w", err)
			}

			if err := p.addEvent(ctx, tx, expiredJob.ID, "requeued_expired", queueName, nil); err != nil {
				return err
			}
		}
	}

	// Also promote delayed jobs
	if err := p.promoteDelayedWithTx(ctx, tx, queueName); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (p *PostgresBackend) PromoteDelayed(queueName string) error {
	ctx := context.Background()

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := p.promoteDelayedWithTx(ctx, tx, queueName); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (p *PostgresBackend) promoteDelayedWithTx(ctx context.Context, tx pgx.Tx, queueName string) error {
	// Update delayed jobs that are now ready
	result, err := tx.Exec(ctx, `
		UPDATE jobs 
		SET available_at = NOW(), updated_at = NOW()
		WHERE queue_name = $1 
		  AND status = 'PENDING' 
		  AND available_at > NOW() 
		  AND available_at <= NOW()
	`, queueName)
	if err != nil {
		return fmt.Errorf("failed to promote delayed jobs: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected > 0 {
		// Add events for promoted jobs (this is simplified - in production you might want individual events)
		if err := p.addEvent(ctx, tx, "bulk", "promoted_delayed", queueName, map[string]interface{}{
			"count": rowsAffected,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (p *PostgresBackend) ListQueues() ([]string, error) {
	ctx := context.Background()

	rows, err := p.pool.Query(ctx, "SELECT DISTINCT queue_name FROM jobs")
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var queueName string
		if err := rows.Scan(&queueName); err != nil {
			return nil, fmt.Errorf("failed to scan queue name: %w", err)
		}
		queues = append(queues, queueName)
	}

	return queues, nil
}

func (p *PostgresBackend) GetQueueLength(queueName string) (int64, error) {
	ctx := context.Background()

	var count int64
	err := p.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM jobs 
		WHERE queue_name = $1 AND status = 'PENDING' AND (available_at IS NULL OR available_at <= NOW())
	`, queueName).Scan(&count)

	if err != nil {
		return 0, fmt.Errorf("failed to get queue length: %w", err)
	}

	return count, nil
}

// Legacy support methods - PostgresBackend doesn't use Redis
func (p *PostgresBackend) Client() *redis.Client {
	// Not applicable for Postgres backend
	return nil
}

func (p *PostgresBackend) Ctx() context.Context {
	return context.Background()
}

// Helper method to add events
func (p *PostgresBackend) addEvent(ctx context.Context, tx pgx.Tx, jobID, eventType, queueName string, eventData map[string]interface{}) error {
	var eventDataJSON []byte
	var err error

	if eventData != nil {
		eventDataJSON, err = json.Marshal(eventData)
		if err != nil {
			return fmt.Errorf("failed to marshal event data: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO job_events (job_id, event_type, queue_name, event_data)
		VALUES ($1, $2, $3, $4)
	`, jobID, eventType, queueName, eventDataJSON)

	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	return nil
}

// Close closes the connection pool
func (p *PostgresBackend) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

// SetRateLimiter sets the rate limiter for this backend.
func (p *PostgresBackend) SetRateLimiter(limiter queue.RateLimiter) {
	if limiter == nil {
		p.rateLimiter = queue.NoopRateLimiter{}
		return
	}
	p.rateLimiter = limiter
}

// GetRateLimiter returns the current rate limiter.
func (p *PostgresBackend) GetRateLimiter() queue.RateLimiter {
	return p.rateLimiter
}
