# API Reference

## Data Structures

### Job Structure (Multi-Tenant)

```go
type Job struct {
    ID          string            `json:"id"`
    TenantID    string            `json:"tenant_id"`     // NEW: Tenant isolation
    QueueName   string            `json:"queue_name"`
    Status      string            `json:"status"`
    Payload     string            `json:"payload"`
    Priority    int               `json:"priority"`
    MaxRetries  int               `json:"max_retries"`
    RetryCount  int               `json:"retry_count"`
    Delivery    string            `json:"delivery"`
    AvailableAt time.Time         `json:"available_at"`
    CreatedAt   time.Time         `json:"created_at"`
    UpdatedAt   time.Time         `json:"updated_at"`
    Metadata    map[string]string `json:"metadata"`
}
```

### Tenant Structure

```go
type Tenant struct {
    ID          string          `json:"id"`
    Name        string          `json:"name"`
    Status      string          `json:"status"`        // active, suspended, deleted
    Quotas      TenantQuotas    `json:"quotas"`
    RateLimit   TenantRateLimit `json:"rate_limit"`
    CreatedAt   time.Time       `json:"created_at"`
    UpdatedAt   time.Time       `json:"updated_at"`
    Metadata    map[string]string `json:"metadata"`
}

type TenantQuotas struct {
    MaxQueueLength    int64 `json:"max_queue_length"`     // Max jobs per queue
    MaxQueues         int   `json:"max_queues"`           // Max queues per tenant
    MaxJobSize        int64 `json:"max_job_size"`         // Max job payload size
    MaxJobsPerDay     int64 `json:"max_jobs_per_day"`     // Daily job limit
    MaxRetentionPeriod int  `json:"max_retention_period"` // Days to retain completed jobs
}

type TenantRateLimit struct {
    RequestsPerSecond float64 `json:"requests_per_second"`
    BurstSize         int     `json:"burst_size"`
    Algorithm         string  `json:"algorithm"` // "token_bucket", "leaky_bucket"
}
```

### User & Role Structure (RBAC)

```go
type User struct {
    ID       string   `json:"id"`
    Username string   `json:"username"`
    Roles    []string `json:"roles"`    // ["admin", "tenant_admin", "viewer"]
    TenantID string   `json:"tenant_id"` // Primary tenant for non-admin users
}

// Permissions
const (
    PermissionViewTenants   = "view_tenants"
    PermissionCreateTenant  = "create_tenant"
    PermissionUpdateTenant  = "update_tenant"
    PermissionDeleteTenant  = "delete_tenant"
    PermissionViewQueues    = "view_queues"
    PermissionManageQueues  = "manage_queues"
    PermissionViewJobs      = "view_jobs"
    PermissionManageJobs    = "manage_jobs"
    PermissionViewMetrics   = "view_metrics"
)
```

## API Endpoints

### Core Job Queue API

#### Job Management
- `POST /jobs` - Enqueue a new job (with tenant support)
- `GET /jobs/:id` - Get job details
- `POST /jobs/:id/ack` - Acknowledge job completion
- `POST /jobs/:id/fail` - Mark job as failed
- `GET /queues` - List all queues (tenant-scoped)
- `GET /queues/:name/length` - Get queue length
- `POST /queues/:name/dequeue` - Dequeue a job from queue

#### Multi-Tenant Job Operations
- `POST /tenants/:tenantId/jobs` - Enqueue job for specific tenant
- `GET /tenants/:tenantId/queues` - List tenant's queues
- `GET /tenants/:tenantId/queues/:name/length` - Get tenant queue length
- `POST /tenants/:tenantId/queues/:name/dequeue` - Dequeue from tenant queue

### Admin API (RBAC Protected)

#### Tenant Management
- `GET /api/tenants` - List all tenants (admin only)
- `POST /api/tenants` - Create new tenant (admin only)
- `GET /api/tenants/:id` - Get tenant details
- `PUT /api/tenants/:id` - Update tenant (admin/tenant-admin)
- `DELETE /api/tenants/:id` - Delete tenant (admin only)

#### Tenant Monitoring
- `GET /api/tenants/:id/usage` - Get tenant usage statistics
- `GET /api/tenants/:id/stats` - Get comprehensive tenant statistics
- `GET /api/tenants/:id/queues` - List tenant queues with lengths
- `GET /api/tenants/:id/queues/:name` - Get specific queue details
- `GET /api/tenants/:id/queues/:name/jobs` - List jobs in queue

#### Rate Limiting Management
- `GET /api/tenants/:id/rate-limits` - Get tenant rate limits
- `PUT /api/tenants/:id/rate-limits` - Update tenant rate limits
- `POST /api/tenants/:id/rate-limits/reset` - Reset rate limit counters

## Example API Usage

### Creating a Tenant

```bash
curl -X POST http://localhost:8080/api/tenants \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "tenant-123",
    "name": "Acme Corp",
    "quotas": {
      "max_queue_length": 10000,
      "max_queues": 50,
      "max_job_size": 1048576,
      "max_jobs_per_day": 100000
    },
    "rate_limit": {
      "requests_per_second": 100,
      "burst_size": 200,
      "algorithm": "token_bucket"
    }
  }'
```

### Enqueueing a Job

```bash
curl -X POST http://localhost:8080/tenants/tenant-123/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "queue_name": "email-processing",
    "payload": "{\"email\": \"user@example.com\", \"template\": \"welcome\"}",
    "priority": 5,
    "max_retries": 3
  }'
```

### Checking Tenant Usage

```bash
curl -X GET http://localhost:8080/api/tenants/tenant-123/usage \
  -H "Authorization: Bearer <tenant-admin-token>"
```

### Dequeuing a Job

```bash
curl -X POST http://localhost:8080/tenants/tenant-123/queues/email-processing/dequeue \
  -H "Content-Type: application/json" \
  -d '{
    "worker_id": "worker-001",
    "visibility_timeout": "300s"
  }'
```

### Updating Tenant Rate Limits

```bash
curl -X PUT http://localhost:8080/api/tenants/tenant-123/rate-limits \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "requests_per_second": 200,
    "burst_size": 400,
    "algorithm": "token_bucket"
  }'
```

## Response Formats

### Successful Job Enqueue Response

```json
{
  "id": "job-uuid-12345",
  "tenant_id": "tenant-123",
  "queue_name": "email-processing",
  "status": "pending",
  "priority": 5,
  "created_at": "2025-09-29T10:00:00Z",
  "available_at": "2025-09-29T10:00:00Z"
}
```

### Tenant Usage Response

```json
{
  "tenant_id": "tenant-123",
  "total_queue_length": 1500,
  "active_queues": 12,
  "jobs_today": 25000,
  "jobs_this_month": 750000,
  "quota_usage": {
    "queue_length_pct": 15.0,
    "active_queues_pct": 24.0,
    "daily_jobs_pct": 25.0
  },
  "rate_limit_usage": {
    "current_tokens": 85,
    "max_tokens": 200,
    "refill_rate": 100
  }
}
```

### Error Response Format

```json
{
  "error": "quota_exceeded",
  "message": "Daily job limit exceeded for tenant",
  "details": {
    "tenant_id": "tenant-123",
    "current_usage": 100000,
    "limit": 100000
  },
  "timestamp": "2025-09-29T10:00:00Z"
}
```

## Authentication

### Bearer Token Authentication

Include the Authorization header with all admin API requests:

```bash
Authorization: Bearer <your-token>
```

### Role-Based Access

- **Admin**: Full access to all tenants and system operations
- **TenantAdmin**: Access to specific tenant management and monitoring
- **Viewer**: Read-only access to tenant information and metrics

## Error Codes

| HTTP Status | Error Code | Description |
|-------------|------------|-------------|
| 400 | `bad_request` | Invalid request format or parameters |
| 401 | `unauthorized` | Missing or invalid authentication token |
| 403 | `forbidden` | Insufficient permissions for operation |
| 404 | `not_found` | Tenant, job, or queue not found |
| 409 | `conflict` | Resource already exists or constraint violation |
| 413 | `quota_exceeded` | Tenant quota limits exceeded |
| 429 | `rate_limited` | Rate limit exceeded for tenant |
| 500 | `internal_error` | Server-side error occurred |