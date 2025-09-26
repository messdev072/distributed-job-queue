package queue

import (
	"context"
	"errors"
	"time"
)

// Tenant represents a tenant in the multi-tenant system
type Tenant struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Status    TenantStatus           `json:"status"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
	Quotas    TenantQuotas           `json:"quotas"`
	RateLimit TenantRateLimit        `json:"rate_limit"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// TenantStatus represents the status of a tenant
type TenantStatus string

const (
	TenantStatusActive    TenantStatus = "ACTIVE"
	TenantStatusSuspended TenantStatus = "SUSPENDED"
	TenantStatusInactive  TenantStatus = "INACTIVE"
)

// TenantQuotas defines resource quotas for a tenant
type TenantQuotas struct {
	MaxQueueLength     int64 `json:"max_queue_length"`     // Maximum jobs per queue
	MaxQueues          int   `json:"max_queues"`           // Maximum number of queues
	MaxJobSize         int64 `json:"max_job_size"`         // Maximum job payload size in bytes
	MaxRetentionPeriod int64 `json:"max_retention_period"` // Maximum job retention in hours
	MaxJobsPerDay      int64 `json:"max_jobs_per_day"`     // Daily job limit
}

// TenantRateLimit defines rate limiting for a tenant
type TenantRateLimit struct {
	QueueRatePerSecond  float64 `json:"queue_rate_per_second"`  // Jobs per second per queue
	QueueBurstSize      int     `json:"queue_burst_size"`       // Burst size per queue
	WorkerRatePerSecond float64 `json:"worker_rate_per_second"` // Jobs per second per worker
	WorkerBurstSize     int     `json:"worker_burst_size"`      // Burst size per worker
	GlobalRatePerSecond float64 `json:"global_rate_per_second"` // Global tenant rate limit
	GlobalBurstSize     int     `json:"global_burst_size"`      // Global burst size
}

// TenantUsage tracks current usage for a tenant
type TenantUsage struct {
	TenantID         string    `json:"tenant_id"`
	JobsToday        int64     `json:"jobs_today"`
	TotalJobs        int64     `json:"total_jobs"`
	ActiveQueues     int       `json:"active_queues"`
	TotalQueueLength int64     `json:"total_queue_length"`
	LastActivity     time.Time `json:"last_activity"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// TenantManager defines the interface for tenant management
type TenantManager interface {
	// Tenant CRUD operations
	CreateTenant(ctx context.Context, tenant *Tenant) error
	GetTenant(ctx context.Context, tenantID string) (*Tenant, error)
	UpdateTenant(ctx context.Context, tenant *Tenant) error
	DeleteTenant(ctx context.Context, tenantID string) error
	ListTenants(ctx context.Context, limit, offset int) ([]*Tenant, error)

	// Quota management
	CheckQuotas(ctx context.Context, tenantID string, job *Job) error
	GetTenantUsage(ctx context.Context, tenantID string) (*TenantUsage, error)
	UpdateUsage(ctx context.Context, tenantID string, delta TenantUsageDelta) error

	// Tenant isolation
	GetTenantKeyspace(tenantID string) string
	ValidateTenantAccess(ctx context.Context, tenantID string, userID string) error
}

// TenantUsageDelta represents changes to tenant usage
type TenantUsageDelta struct {
	JobsDelta         int64 `json:"jobs_delta"`
	QueueLengthDelta  int64 `json:"queue_length_delta"`
	ActiveQueuesDelta int   `json:"active_queues_delta"`
}

// Default tenant quotas
var DefaultTenantQuotas = TenantQuotas{
	MaxQueueLength:     10000,
	MaxQueues:          50,
	MaxJobSize:         1024 * 1024, // 1MB
	MaxRetentionPeriod: 24 * 7,      // 1 week
	MaxJobsPerDay:      100000,
}

// Default tenant rate limits
var DefaultTenantRateLimit = TenantRateLimit{
	QueueRatePerSecond:  10.0,
	QueueBurstSize:      20,
	WorkerRatePerSecond: 5.0,
	WorkerBurstSize:     10,
	GlobalRatePerSecond: 100.0,
	GlobalBurstSize:     200,
}

// Common errors
var (
	ErrTenantNotFound  = errors.New("tenant not found")
	ErrTenantSuspended = errors.New("tenant is suspended")
	ErrQuotaExceeded   = errors.New("tenant quota exceeded")
	ErrUnauthorized    = errors.New("unauthorized access to tenant")
	ErrTenantExists    = errors.New("tenant already exists")
	ErrInvalidTenantID = errors.New("invalid tenant ID")
)

// Helper functions

// NewTenant creates a new tenant with default quotas and rate limits
func NewTenant(id, name string) *Tenant {
	now := time.Now()
	return &Tenant{
		ID:        id,
		Name:      name,
		Status:    TenantStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
		Quotas:    DefaultTenantQuotas,
		RateLimit: DefaultTenantRateLimit,
		Metadata:  make(map[string]interface{}),
	}
}

// IsActive returns true if the tenant is active
func (t *Tenant) IsActive() bool {
	return t.Status == TenantStatusActive
}

// ValidateJob checks if a job is valid for this tenant's quotas
func (t *Tenant) ValidateJob(job *Job) error {
	if !t.IsActive() {
		return ErrTenantSuspended
	}

	// Check job size
	if int64(len(job.Payload)) > t.Quotas.MaxJobSize {
		return ErrQuotaExceeded
	}

	return nil
}

// GetKeyspace returns the Redis keyspace for this tenant
func (t *Tenant) GetKeyspace() string {
	return "tenant:" + t.ID
}

// GetQueueKey returns the full Redis key for a queue in this tenant
func (t *Tenant) GetQueueKey(queueName string) string {
	return t.GetKeyspace() + ":jobs:" + queueName + ":z"
}

// GetRateLimitKey returns the rate limiting key for this tenant
func (t *Tenant) GetRateLimitKey(resourceType, resourceID string) string {
	return t.GetKeyspace() + ":rate:" + resourceType + ":" + resourceID
}
