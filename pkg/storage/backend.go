package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"time"

	"github.com/redis/go-redis/v9"
)

// Backend defines the pluggable storage interface for job queue operations.
type Backend interface {
	// Core job operations
	Enqueue(job *queue.Job) error
	EnqueueWithOpts(job *queue.Job, dedupeKey, idempotencyKey string) error
	Dequeue(queueName string, workerID string, visibility time.Duration) (*queue.Job, error)
	Ack(job *queue.Job) error
	Fail(job *queue.Job, reason string) error

	// Job metadata
	GetJob(id string) (*queue.Job, error)
	UpdateJob(job *queue.Job) error

	// Maintenance operations
	RequeueExpired(queueName string) error
	PromoteDelayed(queueName string) error

	// Admin helpers
	ListQueues() ([]string, error)
	GetQueueLength(queueName string) (int64, error)

	// Multi-tenant operations
	EnqueueTenant(job *queue.Job, tenantID string) error
	DequeueTenant(queueName string, tenantID string, workerID string, visibility time.Duration) (*queue.Job, error)
	ListTenantQueues(tenantID string) ([]string, error)
	GetTenantQueueLength(queueName string, tenantID string) (int64, error)

	// Legacy support for existing code
	Client() *redis.Client
	Ctx() context.Context

	// Lifecycle hooks
	SetHooks(hooks queue.LifecycleHooks)

	// Rate limiting
	SetRateLimiter(limiter queue.RateLimiter)
	GetRateLimiter() queue.RateLimiter

	// Tenant management
	SetTenantManager(manager queue.TenantManager)
	GetTenantManager() queue.TenantManager
}
