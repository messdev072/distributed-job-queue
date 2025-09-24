package worker

import (
	"context"
	m "distributed-job-queue/pkg/metrics"
	"distributed-job-queue/pkg/queue"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Registry struct {
	client redisClientWithCtx
	id     string
}

// minimal subset of redis.Client we use, plus a Context() accessor
type redisClientWithCtx interface {
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Context() context.Context
}

// wrapper adds a Context() to redis.Client using a provided base context
type redisClientWrapper struct {
	base *redis.Client
	ctx  context.Context
}

func (w *redisClientWrapper) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return w.base.HSet(ctx, key, values...)
}
func (w *redisClientWrapper) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return w.base.Expire(ctx, key, expiration)
}
func (w *redisClientWrapper) Context() context.Context { return w.ctx }

// NewRegistry creates a Registry using a redis client with a background context.
// Prefer NewRegistryFromQueue to inherit the app's context.
func NewRegistry(client *redis.Client, id string) *Registry {
	return &Registry{client: &redisClientWrapper{base: client, ctx: context.Background()}, id: id}
}

// NewRegistryFromQueue ties the registry to the queue's client and context.
func NewRegistryFromQueue(q queue.Queue, id string) *Registry {
	return &Registry{client: &redisClientWrapper{base: q.Client(), ctx: q.Ctx()}, id: id}
}

func (r *Registry) StartHeartbeat() {
	host, _ := os.Hostname()
	startedAt := time.Now().Unix()
	go func() {
		ctx := r.client.Context()
		for {
			data := map[string]interface{}{
				"hostname":   host,
				"id":         r.id,
				"started_at": startedAt,
				"last_seen":  time.Now().Unix(),
			}
			r.client.HSet(ctx, fmt.Sprintf("worker:%s", r.id), data)
			r.client.Expire(ctx, fmt.Sprintf("worker:%s", r.id), 10*time.Second)
			m.WorkerHeartbeatsTotal.Inc()
			time.Sleep(5 * time.Second)
		}
	}()
}
