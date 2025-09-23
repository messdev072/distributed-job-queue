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
)

type Job struct {
	ID         string    `json:"id"`
	Status     Status    `json:"status"`
	Payload    string    `json:"payload"`
	QueueName  string    `json:"queue"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	RetryCount int       `json:"retry_count"`
	MaxRetries int       `json:"max_retries"`
}

func NewJob(payload string, queueName string) *Job {
	now := time.Now()
	return &Job{
		ID:         uuid.New().String(),
		Status:     StatusPending,
		Payload:    payload,
		QueueName:  queueName,
		CreatedAt:  now,
		UpdatedAt:  now,
		MaxRetries: 3,
		RetryCount: 0,
	}
}
