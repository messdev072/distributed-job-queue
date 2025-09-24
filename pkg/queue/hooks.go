package queue

import "context"

// LifecycleHooks allows users to receive callbacks on key job transitions.
// Implementations should be fast and non-blocking. Errors should be handled internally.
type LifecycleHooks interface {
	OnEnqueue(ctx context.Context, job *Job)
	OnAck(ctx context.Context, job *Job)
	OnFail(ctx context.Context, job *Job, reason string)
}

// NoopHooks is the default hook implementation that does nothing.
type NoopHooks struct{}

func (NoopHooks) OnEnqueue(ctx context.Context, job *Job)             {}
func (NoopHooks) OnAck(ctx context.Context, job *Job)                 {}
func (NoopHooks) OnFail(ctx context.Context, job *Job, reason string) {}
