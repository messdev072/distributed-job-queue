package persistence

import (
	"context"
	"database/sql"
	q "distributed-job-queue/pkg/queue"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresHooks persists job lifecycle events into a Postgres table.
// Schema (SQL provided separately):
//
//	CREATE TABLE IF NOT EXISTS job_events (
//	  id BIGSERIAL PRIMARY KEY,
//	  job_id TEXT NOT NULL,
//	  queue TEXT NOT NULL,
//	  event TEXT NOT NULL,
//	  payload TEXT,
//	  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
//	);
type PostgresHooks struct {
	DB *sql.DB
}

func NewPostgresHooks(connString string) (*PostgresHooks, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, err
	}
	// reasonable limits
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	// ensure schema exists
	_, _ = db.Exec(`
        CREATE TABLE IF NOT EXISTS job_events (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT NOT NULL,
            queue TEXT NOT NULL,
            event TEXT NOT NULL,
            payload TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS job_events_job_id_idx ON job_events(job_id);
        CREATE INDEX IF NOT EXISTS job_events_queue_idx ON job_events(queue);
        CREATE INDEX IF NOT EXISTS job_events_created_at_idx ON job_events(created_at);
    `)
	return &PostgresHooks{DB: db}, nil
}

func (p *PostgresHooks) OnEnqueue(ctx context.Context, job *q.Job) {
	p.insert(ctx, job, "enqueued", job.Payload)
}
func (p *PostgresHooks) OnAck(ctx context.Context, job *q.Job) {
	p.insert(ctx, job, "ack", "")
}
func (p *PostgresHooks) OnFail(ctx context.Context, job *q.Job, reason string) {
	p.insert(ctx, job, "fail", reason)
}

func (p *PostgresHooks) insert(ctx context.Context, job *q.Job, event, payload string) {
	if p == nil || p.DB == nil {
		return
	}
	// Best-effort; ignore errors to avoid impacting queue operation.
	_, _ = p.DB.ExecContext(ctx, `
        INSERT INTO job_events (job_id, queue, event, payload, created_at)
        VALUES ($1, $2, $3, $4, $5)
    `, job.ID, job.QueueName, event, payload, time.Now())
}
