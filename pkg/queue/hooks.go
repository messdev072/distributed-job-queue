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

// MultiHooks fans out events to multiple hook implementations.
type MultiHooks []LifecycleHooks

func (m MultiHooks) OnEnqueue(ctx context.Context, job *Job) {
	for _, h := range m {
		if h != nil {
			h.OnEnqueue(ctx, job)
		}
	}
}
func (m MultiHooks) OnAck(ctx context.Context, job *Job) {
	for _, h := range m {
		if h != nil {
			h.OnAck(ctx, job)
		}
	}
}
func (m MultiHooks) OnFail(ctx context.Context, job *Job, reason string) {
	for _, h := range m {
		if h != nil {
			h.OnFail(ctx, job, reason)
		}
	}
}
