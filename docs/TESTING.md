# Testing Guide

## Test Suite Overview

The project includes comprehensive test coverage for all components with a focus on multi-tenancy, rate limiting, RBAC, and backend compatibility.

## Running Tests

### All Tests
```bash
# Run all tests
go test ./tests/...

# Run with coverage
go test ./tests/... -cover -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Specific Test Suites
```bash
# Multi-tenancy tests (13+ tests)
go test ./tests/ -run TestMultiTenancy

# Rate limiting tests
go test ./tests/ -run TestRateLimiting

# Admin API tests
go test ./tests/ -run TestAdminAPI

# Backend-specific tests
go test ./tests/ -run TestMemoryBackend
go test ./tests/ -run TestRedisBackend
go test ./tests/ -run TestPostgresBackend
```

### Verbose Output
```bash
# Run with detailed output
go test ./tests/... -v

# Run specific test with details
go test ./tests/ -run TestTenantIsolation -v
```

## Test Categories

### 1. Multi-Tenancy Tests (`multi_tenancy_test.go`)

**Test Coverage:**
- ✅ Tenant isolation verification
- ✅ Cross-tenant data leakage prevention
- ✅ Quota enforcement testing
- ✅ Usage tracking accuracy
- ✅ Tenant CRUD operations
- ✅ Keyspace isolation validation
- ✅ Concurrent tenant operations

**Key Test Cases:**
```go
func TestTenantIsolation(t *testing.T)           // Verifies complete data isolation
func TestTenantQuotaEnforcement(t *testing.T)    // Tests quota limits
func TestTenantUsageTracking(t *testing.T)       // Validates usage metrics
func TestConcurrentTenantOperations(t *testing.T) // Thread safety
func TestTenantKeyspaceIsolation(t *testing.T)   // Keyspace separation
```

### 2. Rate Limiting Tests (`rate_limiting_test.go`)

**Test Coverage:**
- ✅ Token bucket algorithm validation
- ✅ Per-tenant rate limit enforcement
- ✅ Burst capacity testing
- ✅ Rate limit reset functionality
- ✅ Distributed rate limiting consistency
- ✅ Algorithm switching (token bucket vs leaky bucket)

**Key Test Cases:**
```go
func TestTokenBucketRateLimit(t *testing.T)      // Token bucket algorithm
func TestPerTenantRateLimit(t *testing.T)        // Tenant-specific limits
func TestRateLimitBurst(t *testing.T)            // Burst capacity handling
func TestRateLimitReset(t *testing.T)            // Reset functionality
func TestDistributedRateLimit(t *testing.T)      // Multi-instance consistency
```

### 3. Admin API Tests (`admin_api_test.go`)

**Test Coverage:**
- ✅ RBAC permission verification
- ✅ Authentication/authorization flows
- ✅ Tenant management operations
- ✅ API endpoint security testing
- ✅ Role-based access validation
- ✅ Token-based authentication

**Key Test Cases:**
```go
func TestAdminAPIAuthentication(t *testing.T)    // Token validation
func TestRBACPermissions(t *testing.T)           // Role-based access
func TestTenantManagementAPI(t *testing.T)       // CRUD operations
func TestUnauthorizedAccess(t *testing.T)        // Security validation
func TestTenantScopedOperations(t *testing.T)    // Tenant isolation in API
```

### 4. Backend Tests

#### Memory Backend Tests (`memory_backend_test.go`)
- ✅ Thread safety validation
- ✅ Data consistency checks
- ✅ Concurrent access handling
- ✅ Priority queue ordering
- ✅ Job lifecycle management

#### Redis Backend Tests (in main test suite)
- ✅ Atomic operation validation
- ✅ Data persistence verification
- ✅ Lua script correctness
- ✅ Connection handling
- ✅ Keyspace isolation

#### PostgreSQL Backend Tests (`postgres_backend_test.go`)
- ✅ Transaction integrity
- ✅ Concurrent job claiming
- ✅ Schema validation
- ✅ Index usage verification
- ✅ ACID compliance

## Test Environment Setup

### PostgreSQL Testing

```bash
# Using Docker
docker run --name postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=job_queue_test \
  -p 5432:5432 -d postgres:15

# Wait for PostgreSQL to be ready
sleep 5

# Run PostgreSQL tests
TEST_POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/job_queue_test?sslmode=disable" \
  go test ./tests/ -run TestPostgres -v
```

### Redis Testing

```bash
# Using Docker
docker run --name redis-test -p 6379:6379 -d redis:7-alpine

# Run Redis tests
TEST_REDIS_URL="redis://localhost:6379" \
  go test ./tests/ -run TestRedis -v
```

### Clean Test Environment

```bash
# Clean up test containers
docker rm -f postgres-test redis-test

# Clean test data
rm -rf test-data/
rm coverage.out
```

## Load Testing

### Basic Load Testing

```bash
# Install vegeta for load testing
go install github.com/tsenart/vegeta@latest

# Test job enqueueing
echo "POST http://localhost:8080/tenants/test-tenant/jobs" | \
  vegeta attack -duration=30s -rate=100 \
  -header "Content-Type: application/json" \
  -body examples/job_payload.json | \
  vegeta report

# Test admin API
echo "GET http://localhost:8080/api/tenants" | \
  vegeta attack -duration=10s -rate=50 \
  -header "Authorization: Bearer test-admin-token" | \
  vegeta report
```

### Performance Benchmarks

```bash
# Run Go benchmarks
go test ./tests/ -bench=. -benchmem

# Specific backend benchmarks
go test ./tests/ -bench=BenchmarkRedisEnqueue -benchmem
go test ./tests/ -bench=BenchmarkPostgresEnqueue -benchmem
go test ./tests/ -bench=BenchmarkMemoryEnqueue -benchmem
```

## Test Data and Fixtures

### Test Tenant Data

```go
// Example test tenant
testTenant := &queue.Tenant{
    ID:   "test-tenant-123",
    Name: "Test Tenant",
    Quotas: queue.TenantQuotas{
        MaxQueueLength:    1000,
        MaxQueues:         10,
        MaxJobSize:        1024 * 1024, // 1MB
        MaxJobsPerDay:     10000,
        MaxRetentionPeriod: 30,
    },
    RateLimit: queue.TenantRateLimit{
        RequestsPerSecond: 100,
        BurstSize:         200,
        Algorithm:         "token_bucket",
    },
}
```

### Test Job Data

```go
// Example test job
testJob := &queue.Job{
    ID:          "job-test-123",
    TenantID:    "test-tenant-123",
    QueueName:   "test-queue",
    Status:      "pending",
    Payload:     `{"message": "test job"}`,
    Priority:    5,
    MaxRetries:  3,
    RetryCount:  0,
    Delivery:    "at_least_once",
    AvailableAt: time.Now(),
    CreatedAt:   time.Now(),
    UpdatedAt:   time.Now(),
    Metadata: map[string]string{
        "test": "true",
        "env":  "testing",
    },
}
```

## Coverage Reports

### Generating Coverage Reports

```bash
# Generate coverage profile
go test ./tests/... -coverprofile=coverage.out

# View coverage in terminal
go tool cover -func=coverage.out

# Generate HTML coverage report
go tool cover -html=coverage.out -o coverage.html

# Open in browser (Linux)
xdg-open coverage.html
```

### Coverage Targets

- **Overall Coverage**: Target 85%+
- **Multi-Tenancy**: Target 95%+
- **Rate Limiting**: Target 90%+
- **Admin API**: Target 90%+
- **Backend Interfaces**: Target 85%+

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: job_queue_test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
      
      redis:
        image: redis:7-alpine
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 6379:6379
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Go
      uses: actions/setup-go@v3
      with:
        go-version: 1.21
    
    - name: Run tests
      env:
        TEST_POSTGRES_DSN: postgres://postgres:postgres@localhost:5432/job_queue_test?sslmode=disable
        TEST_REDIS_URL: redis://localhost:6379
      run: |
        go test ./tests/... -v -cover -coverprofile=coverage.out
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.out
```

## Test Best Practices

### 1. Test Organization
- Group related tests in the same file
- Use descriptive test names
- Include setup and teardown logic
- Use table-driven tests for multiple scenarios

### 2. Test Data Management
- Use factories for test data creation
- Clean up test data after each test
- Use isolated test databases/instances
- Avoid hardcoded values

### 3. Parallel Testing
```go
func TestSomething(t *testing.T) {
    t.Parallel() // Enable parallel execution
    // Test implementation
}
```

### 4. Test Helpers
```go
// Helper function for creating test tenants
func createTestTenant(t *testing.T, id string) *queue.Tenant {
    t.Helper()
    // Implementation
}
```

### 5. Error Testing
```go
func TestErrorConditions(t *testing.T) {
    // Test both success and failure cases
    // Verify specific error types and messages
}
```

## Debugging Tests

### Verbose Test Output
```bash
# Run with verbose output
go test ./tests/ -v

# Run specific test with extra logging
go test ./tests/ -run TestTenantIsolation -v -args -debug
```

### Test Debugging Tools
```bash
# Run tests with race detection
go test ./tests/... -race

# Run tests with memory profiling
go test ./tests/... -memprofile=mem.prof
go tool pprof mem.prof
```

This comprehensive testing approach ensures the reliability, security, and performance of the distributed job queue system across all components and use cases.