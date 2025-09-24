# Persistence / Durability (Optional)

Enable long-term job history by persisting lifecycle events to Postgres.

## Enable

Set the environment variable before starting the server:

POSTGRES_DSN="postgres://user:pass@host:5432/dbname?sslmode=disable"

The server will automatically create the table and indexes if they don't exist.

## Schema

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

## Notes

- Persistence is best-effort. Failures to write events do not block queue operations.
- Events recorded: enqueued, ack (completed), fail (each failed attempt). Retries also emit an enqueued event when the job re-enters the queue.
