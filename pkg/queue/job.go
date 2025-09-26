package queue

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	StatusCanceled  Status = "CANCELED"
)

type Job struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`     // Multi-tenancy support
	Status       Status    `json:"status"`
	Payload      string    `json:"payload"`
	QueueName    string    `json:"queue"`
	Priority     int       `json:"priority"`
	AvailableAt  time.Time `json:"available_at,omitempty"`
	RecurrenceID string    `json:"recurrence_id,omitempty"`
	Delivery     string    `json:"delivery,omitempty"` // at_least_once (default) or at_most_once
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	RetryCount   int       `json:"retry_count"`
	MaxRetries   int       `json:"max_retries"`
}

func NewJob(payload string, queueName string) *Job {
	now := time.Now()
	return &Job{
		ID:         uuid.New().String(),
		TenantID:   "default", // Default tenant if not specified
		Status:     StatusPending,
		Payload:    payload,
		QueueName:  queueName,
		Priority:   0,
		CreatedAt:  now,
		UpdatedAt:  now,
		MaxRetries: 3,
		RetryCount: 0,
		Delivery:   "at_least_once",
	}
}

// NewJobWithTenant creates a new job with a specific tenant ID
func NewJobWithTenant(payload string, queueName string, tenantID string) *Job {
	job := NewJob(payload, queueName)
	job.TenantID = tenantID
	return job
}
