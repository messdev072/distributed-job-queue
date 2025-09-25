package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// MemoryBackend implements Backend interface using in-memory storage for testing.
type MemoryBackend struct {
	mu          sync.RWMutex
	jobs        map[string]*queue.Job
	queues      map[string]*memoryQueue
	hooks       queue.LifecycleHooks
	events      map[string][]map[string]interface{}
	dedupe      map[string]string // dedupe key -> job ID
	idempotency map[string]string // idempotency key -> result
	ownership   map[string]string // job ID -> worker ID
}

type memoryQueue struct {
	ready      []*queuedJob
	processing map[string]time.Time // job ID -> deadline
	delayed    []*delayedJob
	dlq        []string
}

type queuedJob struct {
	JobID       string
	Priority    int
	EnqueueTime time.Time
}

type delayedJob struct {
	JobID     string
	ReadyTime time.Time
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		jobs:        make(map[string]*queue.Job),
		queues:      make(map[string]*memoryQueue),
		hooks:       queue.NoopHooks{},
		events:      make(map[string][]map[string]interface{}),
		dedupe:      make(map[string]string),
		idempotency: make(map[string]string),
		ownership:   make(map[string]string),
	}
}

func (m *MemoryBackend) SetHooks(hooks queue.LifecycleHooks) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hooks == nil {
		m.hooks = queue.NoopHooks{}
		return
	}
	m.hooks = hooks
}

func (m *MemoryBackend) getQueue(name string) *memoryQueue {
	if q, exists := m.queues[name]; exists {
		return q
	}
	q := &memoryQueue{
		ready:      make([]*queuedJob, 0),
		processing: make(map[string]time.Time),
		delayed:    make([]*delayedJob, 0),
		dlq:        make([]string, 0),
	}
	m.queues[name] = q
	return q
}

func (m *MemoryBackend) Enqueue(job *queue.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs[job.ID] = job
	q := m.getQueue(job.QueueName)

	if !job.AvailableAt.IsZero() && job.AvailableAt.After(time.Now()) {
		// Add to delayed
		q.delayed = append(q.delayed, &delayedJob{
			JobID:     job.ID,
			ReadyTime: job.AvailableAt,
		})
	} else {
		// Add to ready queue
		q.ready = append(q.ready, &queuedJob{
			JobID:       job.ID,
			Priority:    job.Priority,
			EnqueueTime: job.CreatedAt,
		})
		// Sort by priority (higher first), then by enqueue time (earlier first)
		sort.Slice(q.ready, func(i, j int) bool {
			if q.ready[i].Priority != q.ready[j].Priority {
				return q.ready[i].Priority > q.ready[j].Priority
			}
			return q.ready[i].EnqueueTime.Before(q.ready[j].EnqueueTime)
		})
	}

	m.appendEvent(job.ID, map[string]interface{}{
		"type":      "enqueued",
		"queue":     job.QueueName,
		"timestamp": time.Now().Unix(),
	})

	// Fire hook
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		m.hooks.OnEnqueue(context.Background(), j)
	}(job)

	return nil
}

func (m *MemoryBackend) EnqueueWithOpts(job *queue.Job, dedupeKey, idempotencyKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if dedupeKey != "" {
		if existingID, exists := m.dedupe[dedupeKey]; exists {
			if existingJob, ok := m.jobs[existingID]; ok {
				*job = *existingJob
				return nil
			}
		}
		m.dedupe[dedupeKey] = job.ID
	}

	m.mu.Unlock()
	return m.Enqueue(job)
}

func (m *MemoryBackend) Dequeue(queueName string, workerID string, visibility time.Duration) (*queue.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.getQueue(queueName)
	if len(q.ready) == 0 {
		return nil, errors.New("no jobs available")
	}

	// Take first job (highest priority)
	qj := q.ready[0]
	q.ready = q.ready[1:]

	job := m.jobs[qj.JobID]
	if job == nil {
		return nil, errors.New("job not found")
	}

	// Mark as processing
	q.processing[job.ID] = time.Now().Add(visibility)
	if workerID != "" {
		m.ownership[job.ID] = workerID
	}

	job.Status = queue.StatusRunning
	job.UpdatedAt = time.Now()

	m.appendEvent(job.ID, map[string]interface{}{
		"type":      "dequeued",
		"queue":     queueName,
		"timestamp": time.Now().Unix(),
	})

	return job, nil
}

func (m *MemoryBackend) GetJob(id string) (*queue.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	job, exists := m.jobs[id]
	if !exists {
		return nil, errors.New("job not found")
	}
	return job, nil
}

func (m *MemoryBackend) UpdateJob(job *queue.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs[job.ID] = job
	return nil
}

func (m *MemoryBackend) Ack(job *queue.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.getQueue(job.QueueName)
	delete(q.processing, job.ID)
	delete(m.ownership, job.ID)

	job.Status = queue.StatusCompleted
	job.UpdatedAt = time.Now()
	m.jobs[job.ID] = job

	m.appendEvent(job.ID, map[string]interface{}{
		"type":      "ack",
		"queue":     job.QueueName,
		"timestamp": time.Now().Unix(),
	})

	// Fire hook
	go func(j *queue.Job) {
		defer func() { _ = recover() }()
		m.hooks.OnAck(context.Background(), j)
	}(job)

	return nil
}

func (m *MemoryBackend) Fail(job *queue.Job, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job.RetryCount++
	job.UpdatedAt = time.Now()
	job.Status = queue.StatusFailed

	q := m.getQueue(job.QueueName)
	delete(q.processing, job.ID)
	delete(m.ownership, job.ID)

	if job.Delivery == "at_most_once" || job.RetryCount > job.MaxRetries {
		// Send to DLQ
		q.dlq = append(q.dlq, job.ID)
		m.appendEvent(job.ID, map[string]interface{}{
			"type":      "failed_terminal",
			"queue":     job.QueueName,
			"timestamp": time.Now().Unix(),
		})
	} else {
		// Requeue for retry
		q.ready = append(q.ready, &queuedJob{
			JobID:       job.ID,
			Priority:    job.Priority,
			EnqueueTime: time.Now(),
		})
		// Re-sort
		sort.Slice(q.ready, func(i, j int) bool {
			if q.ready[i].Priority != q.ready[j].Priority {
				return q.ready[i].Priority > q.ready[j].Priority
			}
			return q.ready[i].EnqueueTime.Before(q.ready[j].EnqueueTime)
		})
		m.appendEvent(job.ID, map[string]interface{}{
			"type":      "retry_scheduled",
			"queue":     job.QueueName,
			"timestamp": time.Now().Unix(),
		})
	}

	m.jobs[job.ID] = job

	// Fire hook
	go func(j *queue.Job, rsn string) {
		defer func() { _ = recover() }()
		m.hooks.OnFail(context.Background(), j, rsn)
	}(job, reason)

	return nil
}

func (m *MemoryBackend) RequeueExpired(queueName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.getQueue(queueName)
	now := time.Now()

	// Check for expired processing jobs
	for jobID, deadline := range q.processing {
		if now.After(deadline) {
			delete(q.processing, jobID)
			delete(m.ownership, jobID)

			if job, exists := m.jobs[jobID]; exists {
				// For at_most_once delivery mode, if job is already failed, don't requeue
				if job.Delivery == "at_most_once" && job.Status == queue.StatusFailed {
					m.appendEvent(jobID, map[string]interface{}{
						"type":      "expired_at_most_once_not_requeued",
						"queue":     queueName,
						"timestamp": now.Unix(),
					})
					continue
				}

				job.Status = queue.StatusPending
				job.UpdatedAt = now
				q.ready = append(q.ready, &queuedJob{
					JobID:       jobID,
					Priority:    job.Priority,
					EnqueueTime: now,
				})
				m.appendEvent(jobID, map[string]interface{}{
					"type":      "requeued_expired",
					"queue":     queueName,
					"timestamp": now.Unix(),
				})
			}
		}
	}

	// Re-sort ready queue
	sort.Slice(q.ready, func(i, j int) bool {
		if q.ready[i].Priority != q.ready[j].Priority {
			return q.ready[i].Priority > q.ready[j].Priority
		}
		return q.ready[i].EnqueueTime.Before(q.ready[j].EnqueueTime)
	})

	return m.promoteDelayed(queueName, q)
}

func (m *MemoryBackend) PromoteDelayed(queueName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.getQueue(queueName)
	return m.promoteDelayed(queueName, q)
}

// promoteDelayed is the internal helper that doesn't lock (caller must hold lock)
func (m *MemoryBackend) promoteDelayed(queueName string, q *memoryQueue) error {
	now := time.Now()

	promoted := 0
	for i := 0; i < len(q.delayed); i++ {
		if now.After(q.delayed[i].ReadyTime) || now.Equal(q.delayed[i].ReadyTime) {
			jobID := q.delayed[i].JobID
			if job, exists := m.jobs[jobID]; exists {
				q.ready = append(q.ready, &queuedJob{
					JobID:       jobID,
					Priority:    job.Priority,
					EnqueueTime: now,
				})
				job.UpdatedAt = now
				if job.AvailableAt.After(now) {
					job.AvailableAt = now
				}
				m.appendEvent(jobID, map[string]interface{}{
					"type":      "promoted_delayed",
					"queue":     queueName,
					"timestamp": now.Unix(),
				})
			}
			// Remove from delayed
			q.delayed = append(q.delayed[:i], q.delayed[i+1:]...)
			i--
			promoted++
		}
	}

	if promoted > 0 {
		// Re-sort ready queue
		sort.Slice(q.ready, func(i, j int) bool {
			if q.ready[i].Priority != q.ready[j].Priority {
				return q.ready[i].Priority > q.ready[j].Priority
			}
			return q.ready[i].EnqueueTime.Before(q.ready[j].EnqueueTime)
		})
	}

	return nil
}

func (m *MemoryBackend) ListQueues() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queues := make([]string, 0, len(m.queues))
	for name := range m.queues {
		queues = append(queues, name)
	}
	return queues, nil
}

func (m *MemoryBackend) GetQueueLength(queueName string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q := m.getQueue(queueName)
	return int64(len(q.ready)), nil
}

func (m *MemoryBackend) Client() *redis.Client {
	// Not applicable for memory backend
	return nil
}

func (m *MemoryBackend) Ctx() context.Context {
	return context.Background()
}

func (m *MemoryBackend) appendEvent(jobID string, evt map[string]interface{}) {
	if m.events[jobID] == nil {
		m.events[jobID] = make([]map[string]interface{}, 0)
	}
	m.events[jobID] = append(m.events[jobID], evt)
	// Keep last 100
	if len(m.events[jobID]) > 100 {
		m.events[jobID] = m.events[jobID][len(m.events[jobID])-100:]
	}
}
