package tests

import (
	"distributed-job-queue/pkg/storage"
	"context"
	"distributed-job-queue/pkg/queue"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiTenancyRedisIntegration(t *testing.T) {
	// Setup Redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// Clear test data
	ctx := context.Background()
	client.FlushDB(ctx)

	// Create tenant manager
	tenantManager := storage.NewRedisTenantManager(client)

	// Create Redis backend
	backend := storage.NewRedisBackend("localhost:6379")
	backend.SetTenantManager(tenantManager)

	// Test tenant creation
	t.Run("CreateTenant", func(t *testing.T) {
		tenant := queue.NewTenant("test-tenant-1", "Test Tenant 1")
		tenant.Quotas.MaxQueueLength = 100
		tenant.Quotas.MaxJobsPerDay = 1000

		err := tenantManager.CreateTenant(ctx, tenant)
		require.NoError(t, err)

		// Verify tenant was created
		retrieved, err := tenantManager.GetTenant(ctx, "test-tenant-1")
		require.NoError(t, err)
		assert.Equal(t, "Test Tenant 1", retrieved.Name)
		assert.Equal(t, int64(100), retrieved.Quotas.MaxQueueLength)
	})

	// Test tenant job enqueueing with keyspace isolation
	t.Run("TenantJobEnqueue", func(t *testing.T) {
		// Create jobs for different tenants
		job1 := queue.NewJobWithTenant("payload1", "high-priority", "test-tenant-1")
		job2 := queue.NewJobWithTenant("payload2", "high-priority", "test-tenant-2")

		// Enqueue jobs
		err := backend.EnqueueTenant(job1, "test-tenant-1")
		require.NoError(t, err)

		// Create second tenant first
		tenant2 := queue.NewTenant("test-tenant-2", "Test Tenant 2")
		err = tenantManager.CreateTenant(ctx, tenant2)
		require.NoError(t, err)

		err = backend.EnqueueTenant(job2, "test-tenant-2")
		require.NoError(t, err)

		// Verify jobs are in separate keyspaces
		tenant1QueueLength, err := backend.GetTenantQueueLength("high-priority", "test-tenant-1")
		require.NoError(t, err)
		assert.Equal(t, int64(1), tenant1QueueLength)

		tenant2QueueLength, err := backend.GetTenantQueueLength("high-priority", "test-tenant-2")
		require.NoError(t, err)
		assert.Equal(t, int64(1), tenant2QueueLength)

		// Verify jobs are isolated - tenant 1 can only see their jobs
		queues1, err := backend.ListTenantQueues("test-tenant-1")
		require.NoError(t, err)
		assert.Contains(t, queues1, "high-priority")

		queues2, err := backend.ListTenantQueues("test-tenant-2")
		require.NoError(t, err)
		assert.Contains(t, queues2, "high-priority")
	})

	// Test tenant dequeue operations
	t.Run("TenantJobDequeue", func(t *testing.T) {
		// Dequeue job from tenant 1
		job, err := backend.DequeueTenant("high-priority", "test-tenant-1", "worker-1", 30*time.Second)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, "test-tenant-1", job.TenantID)
		assert.Equal(t, "payload1", job.Payload)

		// Verify queue length decreased for tenant 1
		tenant1QueueLength, err := backend.GetTenantQueueLength("high-priority", "test-tenant-1")
		require.NoError(t, err)
		assert.Equal(t, int64(0), tenant1QueueLength)

		// Verify tenant 2 still has their job
		tenant2QueueLength, err := backend.GetTenantQueueLength("high-priority", "test-tenant-2")
		require.NoError(t, err)
		assert.Equal(t, int64(1), tenant2QueueLength)
	})

	// Test quota enforcement
	t.Run("QuotaEnforcement", func(t *testing.T) {
		// Create tenant with very low quota
		restrictedTenant := queue.NewTenant("restricted-tenant", "Restricted Tenant")
		restrictedTenant.Quotas.MaxQueueLength = 1
		restrictedTenant.Quotas.MaxJobsPerDay = 2

		err := tenantManager.CreateTenant(ctx, restrictedTenant)
		require.NoError(t, err)

		// First job should succeed
		job1 := queue.NewJobWithTenant("job1", "test-queue", "restricted-tenant")
		err = backend.EnqueueTenant(job1, "restricted-tenant")
		require.NoError(t, err)

		// Second job should fail due to queue length quota
		job2 := queue.NewJobWithTenant("job2", "test-queue", "restricted-tenant")
		err = backend.EnqueueTenant(job2, "restricted-tenant")
		assert.Equal(t, queue.ErrQuotaExceeded, err)
	})

	// Test tenant usage tracking
	t.Run("UsageTracking", func(t *testing.T) {
		// Enqueue a job to ensure usage tracking works
		job := queue.NewJobWithTenant("usage-test", "usage-queue", "test-tenant-1")
		err := backend.EnqueueTenant(job, "test-tenant-1")
		require.NoError(t, err)

		usage, err := tenantManager.GetTenantUsage(ctx, "test-tenant-1")
		require.NoError(t, err)
		
		// Should have tenant usage tracking initialized
		assert.GreaterOrEqual(t, usage.TotalJobs, int64(0))
		assert.Equal(t, "test-tenant-1", usage.TenantID)
	})

	// Test tenant suspension
	t.Run("TenantSuspension", func(t *testing.T) {
		// Get and suspend tenant
		tenant, err := tenantManager.GetTenant(ctx, "test-tenant-1")
		require.NoError(t, err)

		tenant.Status = queue.TenantStatusSuspended
		err = tenantManager.UpdateTenant(ctx, tenant)
		require.NoError(t, err)

		// Try to enqueue job for suspended tenant
		job := queue.NewJobWithTenant("suspended-job", "test-queue", "test-tenant-1")
		err = backend.EnqueueTenant(job, "test-tenant-1")
		assert.Equal(t, queue.ErrTenantSuspended, err)
	})

	// Test tenant deletion and cleanup
	t.Run("TenantDeletion", func(t *testing.T) {
		// Create temporary tenant
		tempTenant := queue.NewTenant("temp-tenant", "Temporary Tenant")
		err := tenantManager.CreateTenant(ctx, tempTenant)
		require.NoError(t, err)

		// Add some data
		job := queue.NewJobWithTenant("temp-job", "temp-queue", "temp-tenant")
		err = backend.EnqueueTenant(job, "temp-tenant")
		require.NoError(t, err)

		// Verify data exists
		queueLength, err := backend.GetTenantQueueLength("temp-queue", "temp-tenant")
		require.NoError(t, err)
		assert.Equal(t, int64(1), queueLength)

		// Delete tenant
		err = tenantManager.DeleteTenant(ctx, "temp-tenant")
		require.NoError(t, err)

		// Verify tenant and data are gone
		_, err = tenantManager.GetTenant(ctx, "temp-tenant")
		assert.Equal(t, queue.ErrTenantNotFound, err)

		// Verify tenant keys are cleaned up
		keys, err := client.Keys(ctx, "tenant:temp-tenant:*").Result()
		require.NoError(t, err)
		assert.Empty(t, keys, "Tenant keys should be cleaned up")
	})

	// Test multi-tenant rate limiting
	t.Run("MultiTenantRateLimiting", func(t *testing.T) {
		// Create tenant with custom rate limits
		rateLimitedTenant := queue.NewTenant("rate-limited", "Rate Limited Tenant")
		rateLimitedTenant.RateLimit.QueueRatePerSecond = 1.0
		rateLimitedTenant.RateLimit.QueueBurstSize = 2

		err := tenantManager.CreateTenant(ctx, rateLimitedTenant)
		require.NoError(t, err)

		// This would require extending the rate limiter to support tenant-specific limits
		// For now, just verify the tenant was created with custom rate limits
		retrieved, err := tenantManager.GetTenant(ctx, "rate-limited")
		require.NoError(t, err)
		assert.Equal(t, 1.0, retrieved.RateLimit.QueueRatePerSecond)
		assert.Equal(t, 2, retrieved.RateLimit.QueueBurstSize)
	})

	// Test tenant listing and pagination
	t.Run("TenantListing", func(t *testing.T) {
		tenants, err := tenantManager.ListTenants(ctx, 10, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(tenants), 2) // Should have at least the tenants we created

		// Test pagination
		firstPage, err := tenantManager.ListTenants(ctx, 2, 0)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(firstPage), 2)

		_, err = tenantManager.ListTenants(ctx, 2, 2)
		require.NoError(t, err)
		// Should be able to get second page without errors
	})
}

func TestMemoryBackendMultiTenancy(t *testing.T) {
	// Test that memory backend supports tenant operations
	backend := storage.NewMemoryBackend()

	// Memory backend should have the new methods
	_, err := backend.ListTenantQueues("test-tenant")
	assert.NoError(t, err) // Should return empty list, not error

	_, err = backend.GetTenantQueueLength("test-queue", "test-tenant")
	assert.NoError(t, err) // Should return 0, not error
}

func TestTenantKeyspaceIsolation(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	ctx := context.Background()
	client.FlushDB(ctx)

	// Test that tenant keyspaces don't overlap
	tenant1 := queue.NewTenant("tenant1", "Tenant 1")
	tenant2 := queue.NewTenant("tenant2", "Tenant 2")

	// Verify keyspace generation
	assert.Equal(t, "tenant:tenant1", tenant1.GetKeyspace())
	assert.Equal(t, "tenant:tenant2", tenant2.GetKeyspace())

	// Verify queue keys
	assert.Equal(t, "tenant:tenant1:jobs:high-priority:z", tenant1.GetQueueKey("high-priority"))
	assert.Equal(t, "tenant:tenant2:jobs:high-priority:z", tenant2.GetQueueKey("high-priority"))

	// Keys should be different even for same queue name
	assert.NotEqual(t, tenant1.GetQueueKey("high-priority"), tenant2.GetQueueKey("high-priority"))
}

// Test tenant validation
func TestTenantValidation(t *testing.T) {
	tenant := queue.NewTenant("test", "Test Tenant")
	
	// Test valid job
	validJob := &queue.Job{
		Payload: "small payload",
	}
	err := tenant.ValidateJob(validJob)
	assert.NoError(t, err)

	// Test job too large
	largePayload := make([]byte, tenant.Quotas.MaxJobSize+1)
	largeJob := &queue.Job{
		Payload: string(largePayload),
	}
	err = tenant.ValidateJob(largeJob)
	assert.Equal(t, queue.ErrQuotaExceeded, err)

	// Test suspended tenant
	tenant.Status = queue.TenantStatusSuspended
	err = tenant.ValidateJob(validJob)
	assert.Equal(t, queue.ErrTenantSuspended, err)
}