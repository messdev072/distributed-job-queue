package storage

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTenantManager implements tenant management using Redis
type RedisTenantManager struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisTenantManager creates a new Redis-based tenant manager
func NewRedisTenantManager(client *redis.Client) *RedisTenantManager {
	return &RedisTenantManager{
		client: client,
		ctx:    context.Background(),
	}
}

// CreateTenant creates a new tenant
func (tm *RedisTenantManager) CreateTenant(ctx context.Context, tenant *queue.Tenant) error {
	// Check if tenant already exists
	exists, err := tm.client.Exists(ctx, tm.getTenantKey(tenant.ID)).Result()
	if err != nil {
		return fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if exists > 0 {
		return queue.ErrTenantExists
	}

	// Serialize tenant
	data, err := json.Marshal(tenant)
	if err != nil {
		return fmt.Errorf("failed to marshal tenant: %w", err)
	}

	// Store tenant with expiration (optional, for cleanup)
	pipe := tm.client.TxPipeline()
	pipe.Set(ctx, tm.getTenantKey(tenant.ID), data, 0)
	pipe.SAdd(ctx, "tenants:active", tenant.ID)

	// Initialize usage tracking
	usage := &queue.TenantUsage{
		TenantID:         tenant.ID,
		JobsToday:        0,
		TotalJobs:        0,
		ActiveQueues:     0,
		TotalQueueLength: 0,
		LastActivity:     time.Now(),
		UpdatedAt:        time.Now(),
	}
	usageData, _ := json.Marshal(usage)
	pipe.Set(ctx, tm.getUsageKey(tenant.ID), usageData, 0)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create tenant: %w", err)
	}

	return nil
}

// GetTenant retrieves a tenant by ID
func (tm *RedisTenantManager) GetTenant(ctx context.Context, tenantID string) (*queue.Tenant, error) {
	data, err := tm.client.Get(ctx, tm.getTenantKey(tenantID)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, queue.ErrTenantNotFound
		}
		return nil, fmt.Errorf("failed to get tenant: %w", err)
	}

	var tenant queue.Tenant
	if err := json.Unmarshal([]byte(data), &tenant); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tenant: %w", err)
	}

	return &tenant, nil
}

// UpdateTenant updates an existing tenant
func (tm *RedisTenantManager) UpdateTenant(ctx context.Context, tenant *queue.Tenant) error {
	// Check if tenant exists
	exists, err := tm.client.Exists(ctx, tm.getTenantKey(tenant.ID)).Result()
	if err != nil {
		return fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if exists == 0 {
		return queue.ErrTenantNotFound
	}

	// Update timestamp
	tenant.UpdatedAt = time.Now()

	// Serialize and store
	data, err := json.Marshal(tenant)
	if err != nil {
		return fmt.Errorf("failed to marshal tenant: %w", err)
	}

	err = tm.client.Set(ctx, tm.getTenantKey(tenant.ID), data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to update tenant: %w", err)
	}

	return nil
}

// DeleteTenant deletes a tenant and all associated data
func (tm *RedisTenantManager) DeleteTenant(ctx context.Context, tenantID string) error {
	// Get tenant keyspace pattern
	keyspace := "tenant:" + tenantID + ":*"

	// Find all keys for this tenant
	keys, err := tm.client.Keys(ctx, keyspace).Result()
	if err != nil {
		return fmt.Errorf("failed to find tenant keys: %w", err)
	}

	pipe := tm.client.TxPipeline()

	// Delete all tenant data
	if len(keys) > 0 {
		pipe.Del(ctx, keys...)
	}

	// Remove from active tenants set
	pipe.SRem(ctx, "tenants:active", tenantID)

	// Delete tenant metadata
	pipe.Del(ctx, tm.getTenantKey(tenantID))
	pipe.Del(ctx, tm.getUsageKey(tenantID))

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete tenant: %w", err)
	}

	return nil
}

// ListTenants returns a paginated list of tenants
func (tm *RedisTenantManager) ListTenants(ctx context.Context, limit, offset int) ([]*queue.Tenant, error) {
	// Get all active tenant IDs
	tenantIDs, err := tm.client.SMembers(ctx, "tenants:active").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get tenant list: %w", err)
	}

	// Apply pagination
	total := len(tenantIDs)
	if offset >= total {
		return []*queue.Tenant{}, nil
	}

	end := offset + limit
	if end > total {
		end = total
	}

	paginatedIDs := tenantIDs[offset:end]

	// Fetch tenant data
	tenants := make([]*queue.Tenant, 0, len(paginatedIDs))
	for _, tenantID := range paginatedIDs {
		tenant, err := tm.GetTenant(ctx, tenantID)
		if err != nil {
			// Skip tenants that can't be loaded
			continue
		}
		tenants = append(tenants, tenant)
	}

	return tenants, nil
}

// CheckQuotas validates that a job doesn't exceed tenant quotas
func (tm *RedisTenantManager) CheckQuotas(ctx context.Context, tenantID string, job *queue.Job) error {
	// Get tenant
	tenant, err := tm.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}

	// Check tenant status
	if !tenant.IsActive() {
		return queue.ErrTenantSuspended
	}

	// Validate job against tenant quotas
	if err := tenant.ValidateJob(job); err != nil {
		return err
	}

	// Get current usage
	usage, err := tm.GetTenantUsage(ctx, tenantID)
	if err != nil {
		return err
	}

	// Check daily job limit
	if usage.JobsToday >= tenant.Quotas.MaxJobsPerDay {
		return queue.ErrQuotaExceeded
	}

	// Check queue length for specific queue
	queueKey := tenant.GetQueueKey(job.QueueName)
	queueLength, err := tm.client.ZCard(ctx, queueKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check queue length: %w", err)
	}

	if queueLength >= tenant.Quotas.MaxQueueLength {
		return queue.ErrQuotaExceeded
	}

	// Check total number of queues
	if usage.ActiveQueues >= tenant.Quotas.MaxQueues && queueLength == 0 {
		return queue.ErrQuotaExceeded
	}

	return nil
}

// GetTenantUsage returns current usage statistics for a tenant
func (tm *RedisTenantManager) GetTenantUsage(ctx context.Context, tenantID string) (*queue.TenantUsage, error) {
	data, err := tm.client.Get(ctx, tm.getUsageKey(tenantID)).Result()
	if err != nil {
		if err == redis.Nil {
			// Initialize usage if it doesn't exist
			usage := &queue.TenantUsage{
				TenantID:         tenantID,
				JobsToday:        0,
				TotalJobs:        0,
				ActiveQueues:     0,
				TotalQueueLength: 0,
				LastActivity:     time.Now(),
				UpdatedAt:        time.Now(),
			}
			return usage, nil
		}
		return nil, fmt.Errorf("failed to get tenant usage: %w", err)
	}

	var usage queue.TenantUsage
	if err := json.Unmarshal([]byte(data), &usage); err != nil {
		return nil, fmt.Errorf("failed to unmarshal usage: %w", err)
	}

	return &usage, nil
}

// UpdateUsage updates tenant usage statistics
func (tm *RedisTenantManager) UpdateUsage(ctx context.Context, tenantID string, delta queue.TenantUsageDelta) error {
	usageKey := tm.getUsageKey(tenantID)

	// Use Lua script for atomic usage updates
	luaScript := `
		local key = KEYS[1]
		local jobs_delta = tonumber(ARGV[1])
		local queue_length_delta = tonumber(ARGV[2])
		local active_queues_delta = tonumber(ARGV[3])
		local now = tonumber(ARGV[4])
		
		local usage = redis.call('GET', key)
		local data = {}
		
		if usage then
			data = cjson.decode(usage)
		else
			data = {
				tenant_id = ARGV[5],
				jobs_today = 0,
				total_jobs = 0,
				active_queues = 0,
				total_queue_length = 0,
				last_activity = now,
				updated_at = now
			}
		end
		
		-- Check if it's a new day (reset daily counter)
		local last_update = data.updated_at or 0
		local current_day = math.floor(now / 86400)
		local last_day = math.floor(last_update / 86400)
		
		if current_day > last_day then
			data.jobs_today = 0
		end
		
		-- Update counters
		data.jobs_today = math.max(0, data.jobs_today + jobs_delta)
		data.total_jobs = math.max(0, data.total_jobs + jobs_delta)
		data.active_queues = math.max(0, data.active_queues + active_queues_delta)
		data.total_queue_length = math.max(0, data.total_queue_length + queue_length_delta)
		data.last_activity = now
		data.updated_at = now
		
		redis.call('SET', key, cjson.encode(data))
		return data.jobs_today
	`

	now := time.Now().Unix()
	result, err := tm.client.Eval(ctx, luaScript, []string{usageKey},
		delta.JobsDelta, delta.QueueLengthDelta, delta.ActiveQueuesDelta, now, tenantID).Result()

	if err != nil {
		return fmt.Errorf("failed to update usage: %w", err)
	}

	// Check if daily limit would be exceeded
	jobsToday, ok := result.(int64)
	if !ok {
		// Try to convert from float64 (Redis sometimes returns this)
		if f, ok := result.(float64); ok {
			jobsToday = int64(f)
		}
	}

	// Get tenant to check quotas
	tenant, err := tm.GetTenant(ctx, tenantID)
	if err == nil && jobsToday > tenant.Quotas.MaxJobsPerDay {
		return queue.ErrQuotaExceeded
	}

	return nil
}

// GetTenantKeyspace returns the keyspace prefix for a tenant
func (tm *RedisTenantManager) GetTenantKeyspace(tenantID string) string {
	return "tenant:" + tenantID
}

// ValidateTenantAccess validates that a user has access to a tenant
func (tm *RedisTenantManager) ValidateTenantAccess(ctx context.Context, tenantID string, userID string) error {
	// Check if tenant exists
	_, err := tm.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}

	// Check user access permissions (simplified - in real implementation,
	// this would check RBAC rules)
	accessKey := fmt.Sprintf("tenant:%s:users", tenantID)
	isMember, err := tm.client.SIsMember(ctx, accessKey, userID).Result()
	if err != nil {
		return fmt.Errorf("failed to check user access: %w", err)
	}

	if !isMember {
		// Check if user is admin
		isAdmin, err := tm.client.SIsMember(ctx, "admins", userID).Result()
		if err != nil {
			return fmt.Errorf("failed to check admin access: %w", err)
		}
		if !isAdmin {
			return queue.ErrUnauthorized
		}
	}

	return nil
}

// Helper methods

func (tm *RedisTenantManager) getTenantKey(tenantID string) string {
	return "tenant:" + tenantID + ":metadata"
}

func (tm *RedisTenantManager) getUsageKey(tenantID string) string {
	return "tenant:" + tenantID + ":usage"
}

// GetTenantQueueStats returns queue statistics for a tenant
func (tm *RedisTenantManager) GetTenantQueueStats(ctx context.Context, tenantID string) (map[string]int64, error) {
	// Find all queue keys for this tenant
	pattern := "tenant:" + tenantID + ":jobs:*:z"
	keys, err := tm.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to find queue keys: %w", err)
	}

	stats := make(map[string]int64)

	// Get length of each queue
	for _, key := range keys {
		// Extract queue name from key
		parts := strings.Split(key, ":")
		if len(parts) >= 4 {
			queueName := parts[3]
			length, err := tm.client.ZCard(ctx, key).Result()
			if err != nil {
				continue // Skip this queue on error
			}
			stats[queueName] = length
		}
	}

	return stats, nil
}

// CleanupExpiredUsage removes old usage data (can be run periodically)
func (tm *RedisTenantManager) CleanupExpiredUsage(ctx context.Context, olderThanDays int) error {
	cutoff := time.Now().AddDate(0, 0, -olderThanDays).Unix()

	// This is a simplified cleanup - in production, you'd want more sophisticated cleanup
	tenantIDs, err := tm.client.SMembers(ctx, "tenants:active").Result()
	if err != nil {
		return err
	}

	for _, tenantID := range tenantIDs {
		usage, err := tm.GetTenantUsage(ctx, tenantID)
		if err != nil {
			continue
		}

		if usage.UpdatedAt.Unix() < cutoff {
			// Reset usage for inactive tenants
			usage.JobsToday = 0
			usage.TotalQueueLength = 0
			usage.UpdatedAt = time.Now()

			data, _ := json.Marshal(usage)
			tm.client.Set(ctx, tm.getUsageKey(tenantID), data, 0)
		}
	}

	return nil
}
