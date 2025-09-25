package main

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Connect to Redis
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Create backend
	backend := storage.NewRedisBackend("localhost:6379")

	// Configure rate limiting
	rateLimitConfig := queue.RateLimitConfig{
		Enabled:             true,
		QueueRatePerSecond:  5.0, // 5 jobs per second per queue
		QueueBurstSize:      10,  // Allow bursts of up to 10 jobs
		WorkerRatePerSecond: 2.0, // 2 jobs per second per worker
		WorkerBurstSize:     3,   // Allow bursts of up to 3 jobs per worker
	}

	// Create Redis-based rate limiter
	rateLimiter := storage.NewRedisRateLimiter(client, rateLimitConfig)

	// Set rate limiter on backend
	backend.SetRateLimiter(rateLimiter)

	// Create rate-limited worker
	worker := queue.NewRateLimitedWorker("worker-1", backend, rateLimiter)

	// Configure worker
	worker.SetConcurrency(2)
	worker.SetPollInterval(100 * time.Millisecond)

	// Register handlers for different queue types
	worker.RegisterHandler("high-priority", func(ctx context.Context, job *queue.Job) error {
		log.Printf("Processing high-priority job: %s", job.ID)
		time.Sleep(100 * time.Millisecond) // Simulate work
		return nil
	})

	worker.RegisterHandler("low-priority", func(ctx context.Context, job *queue.Job) error {
		log.Printf("Processing low-priority job: %s", job.ID)
		time.Sleep(200 * time.Millisecond) // Simulate work
		return nil
	})

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		if err := worker.Start(ctx); err != nil {
			log.Printf("Worker error: %v", err)
		}
	}()

	// Enqueue jobs for demonstration
	log.Println("Enqueuing jobs...")
	
	// Enqueue high-priority jobs
	for i := 0; i < 15; i++ {
		job := &queue.Job{
			ID:        fmt.Sprintf("high-priority-job-%d", i),
			QueueName: "high-priority",
			Status:    queue.StatusPending,
			Payload:   fmt.Sprintf("High priority payload %d", i),
			Priority:  10,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if err := backend.Enqueue(job); err != nil {
			log.Printf("Failed to enqueue job: %v", err)
		}
	}

	// Enqueue low-priority jobs
	for i := 0; i < 10; i++ {
		job := &queue.Job{
			ID:        fmt.Sprintf("low-priority-job-%d", i),
			QueueName: "low-priority",
			Status:    queue.StatusPending,
			Payload:   fmt.Sprintf("Low priority payload %d", i),
			Priority:  1,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if err := backend.Enqueue(job); err != nil {
			log.Printf("Failed to enqueue job: %v", err)
		}
	}

	log.Println("Jobs enqueued. Worker will process them with rate limiting...")

	// Monitor rate limiter status
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Show current token levels
				highTokens, _ := rateLimiter.GetQueueTokens(ctx, "high-priority")
				lowTokens, _ := rateLimiter.GetQueueTokens(ctx, "low-priority")
				workerTokens, _ := rateLimiter.GetWorkerTokens(ctx, "worker-1-0")

				log.Printf("Rate limiter status - High queue: %.2f tokens, Low queue: %.2f tokens, Worker: %.2f tokens",
					highTokens, lowTokens, workerTokens)
			}
		}
	}()

	// Demonstrate manual rate limit checking
	go func() {
		time.Sleep(5 * time.Second)
		log.Println("\nDemonstrating manual rate limit checking...")

		for i := 0; i < 5; i++ {
			allowed, err := rateLimiter.AllowQueue(ctx, "high-priority")
			if err != nil {
				log.Printf("Error checking rate limit: %v", err)
				continue
			}

			if allowed {
				log.Printf("Manual check %d: Queue rate limit allows processing", i+1)
			} else {
				log.Printf("Manual check %d: Queue rate limit blocks processing", i+1)
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()

	// Wait for jobs to process
	time.Sleep(25 * time.Second)

	// Stop worker gracefully
	log.Println("Stopping worker...")
	worker.Stop()

	// Show final queue lengths
	highQueueLength, _ := backend.GetQueueLength("high-priority")
	lowQueueLength, _ := backend.GetQueueLength("low-priority")

	log.Printf("Final queue lengths - High priority: %d, Low priority: %d", highQueueLength, lowQueueLength)
	log.Println("Example completed!")
}

// Alternative example using memory backend for testing
func memoryExample() {
	log.Println("Running memory backend example...")

	// Create memory backend
	backend := storage.NewMemoryBackend()

	// Configure rate limiting
	rateLimitConfig := queue.RateLimitConfig{
		Enabled:             true,
		QueueRatePerSecond:  3.0, // 3 jobs per second per queue
		QueueBurstSize:      5,   // Allow bursts of up to 5 jobs
		WorkerRatePerSecond: 1.0, // 1 job per second per worker
		WorkerBurstSize:     2,   // Allow bursts of up to 2 jobs per worker
	}

	// Create memory-based rate limiter
	rateLimiter := storage.NewMemoryRateLimiter(rateLimitConfig)

	// Set rate limiter on backend
	backend.SetRateLimiter(rateLimiter)

	// Create rate-limited worker
	worker := queue.NewRateLimitedWorker("memory-worker", backend, rateLimiter)

	// Register handler
	worker.RegisterHandler("test", func(ctx context.Context, job *queue.Job) error {
		log.Printf("Processing job: %s", job.ID)
		return nil
	})

	// Start worker
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		if err := worker.Start(ctx); err != nil {
			log.Printf("Worker error: %v", err)
		}
	}()

	// Enqueue test jobs
	for i := 0; i < 8; i++ {
		job := &queue.Job{
			ID:        fmt.Sprintf("memory-job-%d", i),
			QueueName: "test",
			Status:    queue.StatusPending,
			Payload:   fmt.Sprintf("Test payload %d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if err := backend.Enqueue(job); err != nil {
			log.Printf("Failed to enqueue job: %v", err)
		}
	}

	// Wait and observe rate limiting
	time.Sleep(8 * time.Second)

	worker.Stop()
	log.Println("Memory example completed!")
}
