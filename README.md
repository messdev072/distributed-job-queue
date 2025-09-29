# Distributed Job Queue

A high-performance, enterprise-grade distributed job queue system with comprehensive multi-tenancy, advanced rate limiting, pluggable backends, and administrative capabilities.

## 🚀 Key Features

### Core Job Processing
- **Pluggable Backends**: Redis, PostgreSQL, and in-memory storage
- **Priority Queues**: Jobs processed by priority with efficient sorting
- **Delayed Jobs**: Schedule jobs for future execution
- **Retries & DLQ**: Configurable retry logic with dead letter queue
- **Delivery Guarantees**: At-least-once and at-most-once delivery
- **Job Deduplication**: Prevent duplicates with deduplication keys
- **Atomic Operations**: Thread-safe operations across all backends

### 🏢 Multi-Tenancy & Isolation
- **Complete Tenant Isolation**: Keyspace pattern `tenant:<tenantID>:jobs:<queue>:z`
- **Per-Tenant Quotas**: Configurable limits for resources and usage
- **Tenant Management**: Full CRUD operations with usage tracking
- **Zero Data Leakage**: Verified complete isolation between tenants

### 🚦 Advanced Rate Limiting
- **Per-Tenant Rate Limits**: Token bucket algorithm with burst capacity
- **Multiple Algorithms**: Token bucket, leaky bucket, sliding window
- **Distributed Consistency**: Rate limiting across multiple instances
- **Adaptive Throttling**: Dynamic adjustment based on system load

### 🔐 Admin API & RBAC
- **RESTful Admin Interface**: Complete tenant and system management
- **Role-Based Access Control**: Admin, TenantAdmin, Viewer with granular permissions
- **Secure Authentication**: Bearer token with tenant-scoped authorization
- **Audit Trail**: Complete logging of administrative actions

## 🏗 Quick Architecture Overview

The system uses a layered architecture with complete tenant isolation:

- **Admin API Layer**: RBAC, authentication, tenant management
- **Rate Limiting Layer**: Per-tenant and global rate controls  
- **Job Queue Layer**: Isolated tenant job queues
- **Storage Layer**: Pluggable backends (Redis/PostgreSQL/Memory)

*See [Architecture Documentation](docs/ARCHITECTURE.md) for detailed system design.*

## 🗄 Storage Backends

| Backend | Use Case | Throughput | Features |
|---------|----------|------------|----------|
| **Redis** | Production | 15,000+ jobs/sec | Atomic Lua scripts, high performance |
| **PostgreSQL** | Enterprise | 5,000+ jobs/sec | ACID transactions, full consistency |
| **Memory** | Development | 50,000+ jobs/sec | Zero latency, perfect for testing |

All backends support complete multi-tenancy with isolation and rate limiting.

```bash
# Start with Redis (recommended)
BACKEND=redis REDIS_URL=redis://localhost:6379 ./server

# Start with PostgreSQL  
BACKEND=postgres POSTGRES_DSN="postgres://user:pass@localhost/jobqueue" ./server

# Start with Memory (development)
BACKEND=memory ./server
```

## ⚙️ Configuration

### Key Environment Variables

```bash
# Core Settings
BACKEND=redis|postgres|memory
PORT=8080
LOG_LEVEL=info

# Backend Configuration  
REDIS_URL=redis://localhost:6379
POSTGRES_DSN=postgres://user:pass@localhost/jobqueue

# Multi-Tenancy & Rate Limiting
TENANT_ISOLATION_ENABLED=true
RATE_LIMIT_ENABLED=true
ADMIN_API_ENABLED=true
```

*See [Deployment Guide](docs/DEPLOYMENT.md) for complete configuration reference.*



## 🚀 Getting Started

### Quick Start

```bash
# Clone and build
git clone <repository-url>
cd distributed-job-queue
go build -o server ./cmd/server

# Start with Redis (recommended)
BACKEND=redis REDIS_URL=redis://localhost:6379 ./server

# Or with Memory backend (development)  
BACKEND=memory ./server
```

### Docker Deployment

```bash
# Start complete stack with docker-compose
docker-compose up -d

# Check status
docker-compose ps
```

*See [Deployment Guide](docs/DEPLOYMENT.md) for production setups, Kubernetes, and scaling strategies.*

## 📡 API Usage

### Core Operations

```bash
# Enqueue a job for tenant
curl -X POST http://localhost:8080/tenants/tenant-123/jobs \
  -H "Content-Type: application/json" \
  -d '{"queue_name": "emails", "payload": "...", "priority": 5}'

# Admin: Create tenant
curl -X POST http://localhost:8080/api/tenants \
  -H "Authorization: Bearer <admin-token>" \
  -d '{"id": "tenant-123", "name": "Acme Corp", ...}'

# Check tenant usage
curl -H "Authorization: Bearer <token>" \
  http://localhost:8080/api/tenants/tenant-123/usage
```

*See [API Reference](docs/API_REFERENCE.md) for complete endpoint documentation and data structures.*

## 🧪 Testing

### Test Suite (100% Pass Rate)

```bash
# Run all tests
go test ./tests/...

# Specific test categories
go test ./tests/ -run TestMultiTenancy    # 13+ tenant isolation tests
go test ./tests/ -run TestRateLimiting    # Rate limiting validation
go test ./tests/ -run TestAdminAPI        # RBAC and security tests

# Coverage report
go test ./tests/... -cover -coverprofile=coverage.out
go tool cover -html=coverage.out
```

**Test Coverage:**
- ✅ **Multi-Tenancy**: Complete tenant isolation, quota enforcement
- ✅ **Rate Limiting**: Token bucket, burst capacity, distributed consistency  
- ✅ **RBAC Security**: Permission validation, role-based access
- ✅ **Backend Compatibility**: All three backends pass identical test suites

*See [Testing Guide](docs/TESTING.md) for detailed test setup, load testing, and CI/CD integration.*



## ⚡ Performance & Monitoring

### Performance Benchmarks

| Backend | Throughput | Latency (p99) | Best For |
|---------|------------|---------------|----------|
| **Memory** | 50,000+ jobs/sec | <1ms | Development, Testing |
| **Redis** | 15,000+ jobs/sec | <5ms | High Performance |
| **PostgreSQL** | 5,000+ jobs/sec | <20ms | Enterprise, Compliance |

### Monitoring Features

- **Health Checks**: `/health` endpoint with component status
- **Metrics**: Prometheus-compatible metrics at `/metrics`  
- **Usage Tracking**: Real-time tenant resource monitoring
- **Alerting**: Configurable alerts for quotas and rate limits

## 📚 Documentation

| Guide | Description |
|-------|-------------|
| **[API Reference](docs/API_REFERENCE.md)** | Complete API endpoints, data structures, and examples |
| **[Architecture](docs/ARCHITECTURE.md)** | System design, interfaces, and component interaction |
| **[Multi-Tenancy](docs/MULTI_TENANCY.md)** | Tenant isolation, quotas, and usage tracking |
| **[Rate Limiting](docs/RATE_LIMITING.md)** | Advanced rate limiting algorithms and configuration |
| **[Testing Guide](docs/TESTING.md)** | Comprehensive testing, load testing, and CI/CD |
| **[Deployment Guide](docs/DEPLOYMENT.md)** | Production deployment, scaling, and monitoring |
| **[Pluggable Backends](PLUGGABLE_BACKENDS.md)** | Backend implementation and customization |

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

1. **Fork the repository** and create a feature branch
2. **Write comprehensive tests** for new functionality  
3. **Update documentation** for any API or feature changes
4. **Ensure all tests pass**: `go test ./tests/...`
5. **Follow Go best practices** and run `golangci-lint run`
6. **Submit a pull request** with detailed description

### Code Standards
- **Test Coverage**: Minimum 80% for new code
- **Documentation**: All public functions need doc comments
- **Thread Safety**: All operations must be thread-safe
- **Performance**: Benchmark critical paths

## 📊 Roadmap

### Upcoming Features
- [ ] **GraphQL API**: Advanced querying interface
- [ ] **Job Scheduling**: Cron-like scheduling capabilities
- [ ] **Workflow Engine**: Multi-step job dependencies
- [ ] **Metrics Dashboard**: Built-in monitoring interface
- [ ] **Job Encryption**: End-to-end payload encryption

### Long-term Goals
- [ ] **Kubernetes Operator**: Native K8s integration
- [ ] **Multi-Region Support**: Cross-region replication
- [ ] **ML-Based Optimization**: Intelligent queue management

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Redis Team** - Excellent in-memory database
- **PostgreSQL Community** - Robust relational database
- **Go Community** - Powerful programming language

## 📞 Support

- **Documentation**: Complete guides in `/docs` directory
- **Issues**: Report bugs and request features on GitHub
- **Security**: Report security issues responsibly

---

**Built with ❤️ for enterprise job processing**
