package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// RateLimitedWorker processes jobs from queues while respecting rate limits.
type RateLimitedWorker struct {
	id            string
	backend       WorkerBackend
	rateLimiter   RateLimiter
	handlers      map[string]Handler
	concurrency   int
	pollInterval  time.Duration
	maxRetries    int
	queueNames    []string
	stopCh        chan struct{}
	wg            sync.WaitGroup
	running       bool
	mu            sync.RWMutex
}

// WorkerBackend defines the minimal backend interface needed by the worker
type WorkerBackend interface {
	Dequeue(queueName string, workerID string, visibility time.Duration) (*Job, error)
	Ack(job *Job) error
	Fail(job *Job, reason string) error
	RequeueExpired(queueName string) error
	PromoteDelayed(queueName string) error
}

// Handler defines a function to process jobs
type Handler func(ctx context.Context, job *Job) error

// NewRateLimitedWorker creates a new rate-limited worker.
func NewRateLimitedWorker(id string, backend WorkerBackend, rateLimiter RateLimiter) *RateLimitedWorker {
	return &RateLimitedWorker{
		id:           id,
		backend:      backend,
		rateLimiter:  rateLimiter,
		handlers:     make(map[string]Handler),
		concurrency:  1,
		pollInterval: 1 * time.Second,
		maxRetries:   3,
		stopCh:       make(chan struct{}),
	}
}

// RegisterHandler registers a handler for jobs from a specific queue.
func (w *RateLimitedWorker) RegisterHandler(queueName string, handler Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.handlers[queueName] = handler
	
	// Add to queue names if not already present
	for _, name := range w.queueNames {
		if name == queueName {
			return
		}
	}
	w.queueNames = append(w.queueNames, queueName)
}

// SetConcurrency sets the number of concurrent job processors.
func (w *RateLimitedWorker) SetConcurrency(concurrency int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if concurrency < 1 {
		concurrency = 1
	}
	w.concurrency = concurrency
}

// SetPollInterval sets how often to check for new jobs.
func (w *RateLimitedWorker) SetPollInterval(interval time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	w.pollInterval = interval
}

// Start begins processing jobs.
func (w *RateLimitedWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("worker %s is already running", w.id)
	}
	w.running = true
	w.mu.Unlock()

	// Start maintenance goroutine
	w.wg.Add(1)
	go w.maintenanceLoop(ctx)

	// Start worker goroutines
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.processLoop(ctx, i)
	}

	log.Printf("Rate-limited worker %s started with %d goroutines", w.id, w.concurrency)
	return nil
}

// Stop gracefully stops the worker.
func (w *RateLimitedWorker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	w.mu.Unlock()

	close(w.stopCh)
	w.wg.Wait()
	log.Printf("Rate-limited worker %s stopped", w.id)
}

// processLoop is the main processing loop for each worker goroutine.
func (w *RateLimitedWorker) processLoop(ctx context.Context, workerIndex int) {
	defer w.wg.Done()
	
	workerID := fmt.Sprintf("%s-%d", w.id, workerIndex)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.processOneJob(ctx, workerID)
		}
	}
}

// processOneJob attempts to process a single job from any queue.
func (w *RateLimitedWorker) processOneJob(ctx context.Context, workerID string) {
	w.mu.RLock()
	queueNames := make([]string, len(w.queueNames))
	copy(queueNames, w.queueNames)
	handlers := make(map[string]Handler)
	for k, v := range w.handlers {
		handlers[k] = v
	}
	w.mu.RUnlock()

	// Try each queue in order
	for _, queueName := range queueNames {
		// Check rate limits before attempting to dequeue
		allowed, err := w.rateLimiter.AllowQueue(ctx, queueName)
		if err != nil {
			log.Printf("Error checking queue rate limit for %s: %v", queueName, err)
			continue
		}
		if !allowed {
			continue // Queue is rate limited
		}

		allowed, err = w.rateLimiter.AllowWorker(ctx, workerID)
		if err != nil {
			log.Printf("Error checking worker rate limit for %s: %v", workerID, err)
			continue
		}
		if !allowed {
			continue // Worker is rate limited
		}

		// Try to dequeue a job
		job, err := w.backend.Dequeue(queueName, workerID, 30*time.Second)
		if err != nil || job == nil {
			continue // No job available or dequeue failed
		}

		// Process the job
		handler, exists := handlers[queueName]
		if !exists {
			log.Printf("No handler registered for queue %s, failing job %s", queueName, job.ID)
			_ = w.backend.Fail(job, "No handler registered for queue")
			continue
		}

		// Execute handler with timeout
		jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err = handler(jobCtx, job)
		cancel()

		if err != nil {
			log.Printf("Job %s failed: %v", job.ID, err)
			_ = w.backend.Fail(job, err.Error())
		} else {
			log.Printf("Job %s completed successfully", job.ID)
			_ = w.backend.Ack(job)
		}

		return // Successfully processed a job, return to allow rate limiter to update
	}
}

// maintenanceLoop handles queue maintenance tasks.
func (w *RateLimitedWorker) maintenanceLoop(ctx context.Context) {
	defer w.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second) // Run maintenance every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.runMaintenance()
		}
	}
}

// runMaintenance performs periodic maintenance on all queues.
func (w *RateLimitedWorker) runMaintenance() {
	w.mu.RLock()
	queueNames := make([]string, len(w.queueNames))
	copy(queueNames, w.queueNames)
	w.mu.RUnlock()

	for _, queueName := range queueNames {
		// Requeue expired jobs
		if err := w.backend.RequeueExpired(queueName); err != nil {
			log.Printf("Error requeuing expired jobs for queue %s: %v", queueName, err)
		}

		// Promote delayed jobs
		if err := w.backend.PromoteDelayed(queueName); err != nil {
			log.Printf("Error promoting delayed jobs for queue %s: %v", queueName, err)
		}
	}
}
