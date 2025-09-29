# Deployment Guide

## Quick Start Deployment

### Docker Deployment

#### Single Container (Development)
```bash
# Build the image
docker build -t job-queue:latest .

# Run with memory backend
docker run -p 8080:8080 -e BACKEND=memory job-queue:latest

# Run with Redis backend
docker run -p 8080:8080 \
  -e BACKEND=redis \
  -e REDIS_URL=redis://your-redis-host:6379 \
  job-queue:latest
```

#### Docker Compose (Recommended)
```bash
# Clone repository
git clone <repository-url>
cd distributed-job-queue

# Start complete stack
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f job-queue-server
```

### Kubernetes Deployment

#### Basic Deployment
```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: job-queue
  labels:
    app: job-queue
spec:
  replicas: 3
  selector:
    matchLabels:
      app: job-queue
  template:
    metadata:
      labels:
        app: job-queue
    spec:
      containers:
      - name: job-queue
        image: job-queue:latest
        ports:
        - containerPort: 8080
        env:
        - name: BACKEND
          value: "redis"
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        - name: RATE_LIMIT_ENABLED
          value: "true"
        - name: TENANT_ISOLATION_ENABLED
          value: "true"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: job-queue-service
spec:
  selector:
    app: job-queue
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8080
  type: LoadBalancer
```

## Production Configurations

### High Availability Setup (Redis)

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  redis-master:
    image: redis:7-alpine
    command: redis-server --appendonly yes --replica-announce-ip redis-master
    volumes:
      - redis_master_data:/data
    networks:
      - job-queue-network

  redis-replica:
    image: redis:7-alpine
    command: redis-server --replicaof redis-master 6379 --replica-announce-ip redis-replica
    depends_on:
      - redis-master
    networks:
      - job-queue-network

  job-queue:
    build: .
    deploy:
      replicas: 5
      update_config:
        parallelism: 2
        delay: 10s
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    environment:
      - BACKEND=redis
      - REDIS_URL=redis://redis-master:6379
      - REDIS_POOL_SIZE=50
      - WORKER_CONCURRENCY=100
      - RATE_LIMIT_ENABLED=true
      - TENANT_ISOLATION_ENABLED=true
      - LOG_LEVEL=info
    ports:
      - "8080-8084:8080"
    depends_on:
      - redis-master
    networks:
      - job-queue-network
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - job-queue
    networks:
      - job-queue-network

volumes:
  redis_master_data:

networks:
  job-queue-network:
    driver: bridge
```

### PostgreSQL Production Setup

```yaml
# docker-compose.postgres-prod.yml
version: '3.8'

services:
  postgres-primary:
    image: postgres:15
    environment:
      POSTGRES_DB: jobqueue
      POSTGRES_USER: jobqueue
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_REPLICATION_MODE: master
      POSTGRES_REPLICATION_USER: replica_user
      POSTGRES_REPLICATION_PASSWORD: ${REPLICA_PASSWORD}
    volumes:
      - postgres_primary_data:/var/lib/postgresql/data
      - ./postgres/postgresql.conf:/etc/postgresql/postgresql.conf
    command: postgres -c config_file=/etc/postgresql/postgresql.conf
    networks:
      - job-queue-network

  postgres-replica:
    image: postgres:15
    environment:
      POSTGRES_REPLICATION_MODE: slave
      POSTGRES_REPLICATION_USER: replica_user
      POSTGRES_REPLICATION_PASSWORD: ${REPLICA_PASSWORD}
      POSTGRES_MASTER_SERVER: postgres-primary
    depends_on:
      - postgres-primary
    networks:
      - job-queue-network

  job-queue:
    build: .
    deploy:
      replicas: 3
    environment:
      - BACKEND=postgres
      - POSTGRES_DSN=postgres://jobqueue:${POSTGRES_PASSWORD}@postgres-primary:5432/jobqueue?sslmode=require
      - POSTGRES_MAX_CONNS=25
      - POSTGRES_MIN_CONNS=5
      - RATE_LIMIT_ENABLED=true
      - TENANT_ISOLATION_ENABLED=true
    depends_on:
      - postgres-primary
    networks:
      - job-queue-network

volumes:
  postgres_primary_data:

networks:
  job-queue-network:
    driver: bridge
```

## Environment Configuration

### Production Environment Variables

```bash
# Core Configuration
BACKEND=redis|postgres|memory
PORT=8080
LOG_LEVEL=info
WORKER_CONCURRENCY=20

# Redis Configuration
REDIS_URL=redis://redis-cluster:6379
REDIS_POOL_SIZE=50
REDIS_TIMEOUT=5s

# PostgreSQL Configuration
POSTGRES_DSN=postgres://user:pass@host:5432/dbname?sslmode=require
POSTGRES_MAX_CONNS=25
POSTGRES_MIN_CONNS=5
POSTGRES_CONN_MAX_LIFETIME=1h

# Multi-Tenancy
TENANT_ISOLATION_ENABLED=true
DEFAULT_TENANT_QUOTA_MAX_QUEUE_LENGTH=10000
DEFAULT_TENANT_QUOTA_MAX_QUEUES=50
DEFAULT_TENANT_QUOTA_MAX_JOB_SIZE=1048576
DEFAULT_TENANT_QUOTA_MAX_JOBS_PER_DAY=100000

# Rate Limiting
RATE_LIMIT_ENABLED=true
DEFAULT_RATE_LIMIT_RPS=100
DEFAULT_RATE_LIMIT_BURST=200
RATE_LIMIT_ALGORITHM=token_bucket

# Admin API
ADMIN_API_ENABLED=true
ADMIN_API_AUTH_REQUIRED=true
JWT_SECRET_KEY=${JWT_SECRET_KEY}

# Monitoring
METRICS_ENABLED=true
METRICS_PORT=9090
HEALTH_CHECK_INTERVAL=30s

# Security
TLS_ENABLED=false
TLS_CERT_FILE=/etc/ssl/certs/server.crt
TLS_KEY_FILE=/etc/ssl/private/server.key
CORS_ENABLED=true
CORS_ALLOWED_ORIGINS=https://admin.example.com
```

### Configuration Files

#### nginx.conf (Load Balancer)
```nginx
events {
    worker_connections 1024;
}

http {
    upstream job_queue_backend {
        server job-queue-1:8080;
        server job-queue-2:8080;
        server job-queue-3:8080;
        server job-queue-4:8080;
        server job-queue-5:8080;
    }

    server {
        listen 80;
        server_name job-queue.example.com;

        location / {
            proxy_pass http://job_queue_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            
            # Health check exclusion
            location /health {
                access_log off;
                proxy_pass http://job_queue_backend;
            }
        }
    }

    # HTTPS configuration
    server {
        listen 443 ssl http2;
        server_name job-queue.example.com;

        ssl_certificate /etc/nginx/ssl/server.crt;
        ssl_certificate_key /etc/nginx/ssl/server.key;
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_ciphers HIGH:!aNULL:!MD5;

        location / {
            proxy_pass http://job_queue_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto https;
        }
    }
}
```

## Monitoring and Observability

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'job-queue'
    static_configs:
      - targets: ['job-queue-1:9090', 'job-queue-2:9090', 'job-queue-3:9090']
    metrics_path: /metrics
    scrape_interval: 10s

  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "Job Queue Metrics",
    "panels": [
      {
        "title": "Jobs per Second",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(jobs_enqueued_total[5m])",
            "legendFormat": "Enqueued"
          },
          {
            "expr": "rate(jobs_dequeued_total[5m])",
            "legendFormat": "Dequeued"
          }
        ]
      },
      {
        "title": "Queue Lengths by Tenant",
        "type": "graph",
        "targets": [
          {
            "expr": "queue_length",
            "legendFormat": "{{tenant_id}} - {{queue_name}}"
          }
        ]
      },
      {
        "title": "Rate Limit Usage",
        "type": "graph",
        "targets": [
          {
            "expr": "rate_limit_usage_percentage",
            "legendFormat": "{{tenant_id}}"
          }
        ]
      }
    ]
  }
}
```

## Health Checks and Monitoring

### Health Check Endpoint

The `/health` endpoint provides detailed system status:

```json
{
  "status": "healthy",
  "timestamp": "2025-09-29T10:00:00Z",
  "components": {
    "backend": {
      "status": "healthy",
      "type": "redis",
      "latency_ms": 2.5
    },
    "rate_limiter": {
      "status": "healthy",
      "active_limiters": 150
    },
    "tenant_manager": {
      "status": "healthy",
      "active_tenants": 25
    }
  },
  "metrics": {
    "total_jobs": 1500000,
    "jobs_per_second": 250.5,
    "average_latency_ms": 12.3,
    "error_rate": 0.002
  }
}
```

### Kubernetes Health Checks

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
```

## Scaling Strategies

### Horizontal Pod Autoscaler (HPA)

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: job-queue-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: job-queue
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: jobs_per_second
      target:
        type: AverageValue
        averageValue: "100"
```

### Vertical Pod Autoscaler (VPA)

```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: job-queue-vpa
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: job-queue
  updatePolicy:
    updateMode: "Auto"
  resourcePolicy:
    containerPolicies:
    - containerName: job-queue
      maxAllowed:
        cpu: 2
        memory: 4Gi
      minAllowed:
        cpu: 100m
        memory: 128Mi
```

## Security Considerations

### TLS Configuration

```bash
# Generate self-signed certificates (development only)
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes

# Production: Use Let's Encrypt or proper CA-signed certificates
```

### Network Security

```yaml
# NetworkPolicy for Kubernetes
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: job-queue-network-policy
spec:
  podSelector:
    matchLabels:
      app: job-queue
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: job-queue-namespace
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: database-namespace
    ports:
    - protocol: TCP
      port: 5432  # PostgreSQL
    - protocol: TCP
      port: 6379  # Redis
```

This deployment guide covers everything from simple development setups to production-grade deployments with high availability, monitoring, and security considerations.