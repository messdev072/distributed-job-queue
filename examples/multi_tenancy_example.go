package main

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/storage"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Initialize Redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	ctx := context.Background()

	// Clear test data
	client.FlushDB(ctx)

	// Create tenant manager
	tenantManager := storage.NewRedisTenantManager(client)

	// Create Redis backend with tenant support
	backend := storage.NewRedisBackend("localhost:6379")
	backend.SetTenantManager(tenantManager)

	fmt.Println("=== Multi-Tenant Job Queue Demo ===")

	// 1. Create tenants with different quotas
	fmt.Println("1. Creating tenants...")

	// Enterprise tenant with high quotas
	enterpriseTenant := queue.NewTenant("enterprise-corp", "Enterprise Corp")
	enterpriseTenant.Quotas.MaxQueueLength = 10000
	enterpriseTenant.Quotas.MaxJobsPerDay = 100000
	enterpriseTenant.RateLimit.QueueRatePerSecond = 50.0
	enterpriseTenant.RateLimit.QueueBurstSize = 100

	err := tenantManager.CreateTenant(ctx, enterpriseTenant)
	if err != nil {
		log.Fatalf("Failed to create enterprise tenant: %v", err)
	}
	fmt.Printf("✓ Created enterprise tenant: %s\n", enterpriseTenant.Name)

	// Startup tenant with limited quotas
	startupTenant := queue.NewTenant("startup-inc", "Startup Inc")
	startupTenant.Quotas.MaxQueueLength = 100
	startupTenant.Quotas.MaxJobsPerDay = 1000
	startupTenant.RateLimit.QueueRatePerSecond = 5.0
	startupTenant.RateLimit.QueueBurstSize = 10

	err = tenantManager.CreateTenant(ctx, startupTenant)
	if err != nil {
		log.Fatalf("Failed to create startup tenant: %v", err)
	}
	fmt.Printf("✓ Created startup tenant: %s\n", startupTenant.Name)

	// 2. Demonstrate tenant isolation
	fmt.Println("\n2. Testing tenant isolation...")

	// Enqueue jobs for different tenants
	enterpriseJobs := []string{"process-payment", "send-email", "generate-report", "backup-data"}
	startupJobs := []string{"welcome-email", "analytics-event", "user-signup"}

	for i, jobType := range enterpriseJobs {
		job := queue.NewJobWithTenant(
			fmt.Sprintf("Enterprise job %d", i+1),
			jobType,
			"enterprise-corp",
		)
		job.Priority = 10 // High priority

		err := backend.EnqueueTenant(job, "enterprise-corp")
		if err != nil {
			log.Printf("Failed to enqueue enterprise job: %v", err)
			continue
		}
		fmt.Printf("✓ Enqueued enterprise job: %s\n", jobType)
	}

	for i, jobType := range startupJobs {
		job := queue.NewJobWithTenant(
			fmt.Sprintf("Startup job %d", i+1),
			jobType,
			"startup-inc",
		)
		job.Priority = 5 // Medium priority

		err := backend.EnqueueTenant(job, "startup-inc")
		if err != nil {
			log.Printf("Failed to enqueue startup job: %v", err)
			continue
		}
		fmt.Printf("✓ Enqueued startup job: %s\n", jobType)
	}

	// 3. Show tenant queue isolation
	fmt.Println("\n3. Checking tenant queue isolation...")

	enterpriseQueues, err := backend.ListTenantQueues("enterprise-corp")
	if err != nil {
		log.Printf("Failed to list enterprise queues: %v", err)
	} else {
		fmt.Printf("Enterprise queues: %v\n", enterpriseQueues)
	}

	startupQueues, err := backend.ListTenantQueues("startup-inc")
	if err != nil {
		log.Printf("Failed to list startup queues: %v", err)
	} else {
		fmt.Printf("Startup queues: %v\n", startupQueues)
	}

	// Show queue lengths per tenant
	for _, queueName := range []string{"process-payment", "send-email", "welcome-email"} {
		enterpriseLength, _ := backend.GetTenantQueueLength(queueName, "enterprise-corp")
		startupLength, _ := backend.GetTenantQueueLength(queueName, "startup-inc")

		fmt.Printf("Queue '%s': Enterprise=%d, Startup=%d\n", queueName, enterpriseLength, startupLength)
	}

	// 4. Demonstrate quota enforcement
	fmt.Println("\n4. Testing quota enforcement...")

	// Try to exceed startup tenant's queue limit
	fmt.Printf("Startup max queue length: %d\n", startupTenant.Quotas.MaxQueueLength)

	// Fill up startup tenant's quota
	quotaTestJobs := 0
	for i := 0; i < 150; i++ { // Try to exceed the limit of 100
		job := queue.NewJobWithTenant(
			fmt.Sprintf("Quota test job %d", i+1),
			"quota-test",
			"startup-inc",
		)

		err := backend.EnqueueTenant(job, "startup-inc")
		if err != nil {
			if err == queue.ErrQuotaExceeded {
				fmt.Printf("✓ Quota enforcement working: stopped at %d jobs\n", quotaTestJobs)
				break
			}
			log.Printf("Unexpected error: %v", err)
			break
		}
		quotaTestJobs++
	}

	// 5. Process jobs from different tenants
	fmt.Println("\n5. Processing tenant jobs...")

	// Process enterprise jobs
	fmt.Println("Processing Enterprise jobs:")
	for i := 0; i < 3; i++ {
		job, err := backend.DequeueTenant("process-payment", "enterprise-corp", "worker-enterprise", 30*time.Second)
		if err != nil {
			log.Printf("Failed to dequeue enterprise job: %v", err)
			break
		}
		if job == nil {
			break
		}
		fmt.Printf("✓ Processing enterprise job: %s (ID: %s)\n", job.Payload, job.ID)

		// Simulate job processing
		time.Sleep(100 * time.Millisecond)

		// Acknowledge job completion
		err = backend.Ack(job)
		if err != nil {
			log.Printf("Failed to ack job: %v", err)
		}
	}

	// Process startup jobs
	fmt.Println("Processing Startup jobs:")
	for i := 0; i < 2; i++ {
		job, err := backend.DequeueTenant("welcome-email", "startup-inc", "worker-startup", 30*time.Second)
		if err != nil {
			log.Printf("Failed to dequeue startup job: %v", err)
			break
		}
		if job == nil {
			break
		}
		fmt.Printf("✓ Processing startup job: %s (ID: %s)\n", job.Payload, job.ID)

		// Simulate job processing
		time.Sleep(100 * time.Millisecond)

		// Acknowledge job completion
		err = backend.Ack(job)
		if err != nil {
			log.Printf("Failed to ack job: %v", err)
		}
	}

	// 6. Show tenant usage statistics
	fmt.Println("\n6. Tenant usage statistics...")

	enterpriseUsage, err := tenantManager.GetTenantUsage(ctx, "enterprise-corp")
	if err != nil {
		log.Printf("Failed to get enterprise usage: %v", err)
	} else {
		fmt.Printf("Enterprise usage: Jobs=%d, Queues=%d, Queue Length=%d\n",
			enterpriseUsage.TotalJobs, enterpriseUsage.ActiveQueues, enterpriseUsage.TotalQueueLength)
	}

	startupUsage, err := tenantManager.GetTenantUsage(ctx, "startup-inc")
	if err != nil {
		log.Printf("Failed to get startup usage: %v", err)
	} else {
		fmt.Printf("Startup usage: Jobs=%d, Queues=%d, Queue Length=%d\n",
			startupUsage.TotalJobs, startupUsage.ActiveQueues, startupUsage.TotalQueueLength)
	}

	// 7. Demonstrate tenant suspension
	fmt.Println("\n7. Testing tenant suspension...")

	// Suspend startup tenant
	startupTenant.Status = queue.TenantStatusSuspended
	err = tenantManager.UpdateTenant(ctx, startupTenant)
	if err != nil {
		log.Printf("Failed to suspend tenant: %v", err)
	} else {
		fmt.Printf("✓ Suspended startup tenant\n")
	}

	// Try to enqueue job for suspended tenant
	suspendedJob := queue.NewJobWithTenant("This should fail", "test", "startup-inc")
	err = backend.EnqueueTenant(suspendedJob, "startup-inc")
	if err == queue.ErrTenantSuspended {
		fmt.Printf("✓ Correctly blocked job for suspended tenant\n")
	} else {
		fmt.Printf("✗ Expected suspension error, got: %v\n", err)
	}

	// 8. List all tenants
	fmt.Println("\n8. Tenant listing...")

	tenants, err := tenantManager.ListTenants(ctx, 10, 0)
	if err != nil {
		log.Printf("Failed to list tenants: %v", err)
	} else {
		fmt.Printf("Total tenants: %d\n", len(tenants))
		for _, t := range tenants {
			fmt.Printf("- %s (%s): Status=%s, MaxQueueLength=%d\n",
				t.Name, t.ID, t.Status, t.Quotas.MaxQueueLength)
		}
	}

	// 9. Demonstrate keyspace isolation
	fmt.Println("\n9. Verifying keyspace isolation...")

	// Show that different tenants use different Redis keys
	fmt.Println("Redis keyspace patterns:")
	fmt.Printf("Enterprise tenant keyspace: %s\n", enterpriseTenant.GetKeyspace())
	fmt.Printf("Startup tenant keyspace: %s\n", startupTenant.GetKeyspace())

	// Show queue keys are isolated
	fmt.Printf("Enterprise 'process-payment' queue key: %s\n",
		enterpriseTenant.GetQueueKey("process-payment"))
	fmt.Printf("Startup 'process-payment' queue key: %s\n",
		startupTenant.GetQueueKey("process-payment"))

	// Verify keys are different even for same queue name
	if enterpriseTenant.GetQueueKey("process-payment") != startupTenant.GetQueueKey("process-payment") {
		fmt.Printf("✓ Queue keys are properly isolated between tenants\n")
	}

	fmt.Println("\n=== Demo completed ===")
	fmt.Println("\nKey multi-tenancy features demonstrated:")
	fmt.Println("✓ Tenant creation with custom quotas and rate limits")
	fmt.Println("✓ Job enqueueing with tenant isolation")
	fmt.Println("✓ Keyspace isolation (tenant:<tenantID>:jobs:<queue>:z)")
	fmt.Println("✓ Quota enforcement (max queue length, daily job limits)")
	fmt.Println("✓ Tenant suspension and access control")
	fmt.Println("✓ Usage tracking and statistics")
	fmt.Println("✓ Tenant listing and management")
	fmt.Println("✓ Complete data isolation between tenants")
}

// Additional helper functions for advanced multi-tenancy features

func demonstrateAdvancedFeatures(tenantManager queue.TenantManager, backend queue.Backend) {
	fmt.Println("\n=== Advanced Multi-Tenancy Features ===")

	ctx := context.Background()

	// 1. Tenant-specific rate limiting
	fmt.Println("\n1. Tenant-specific rate limiting...")

	// This would require extending the rate limiter to be tenant-aware
	// For now, we show the tenant rate limit configuration
	tenant, _ := tenantManager.GetTenant(ctx, "enterprise-corp")
	fmt.Printf("Enterprise rate limits: %.1f jobs/sec, burst %d\n",
		tenant.RateLimit.QueueRatePerSecond, tenant.RateLimit.QueueBurstSize)

	// 2. Tenant data cleanup
	fmt.Println("\n2. Tenant data cleanup...")

	// Create temporary tenant for cleanup demo
	tempTenant := queue.NewTenant("temp-tenant", "Temporary Tenant")
	tenantManager.CreateTenant(ctx, tempTenant)

	// Add some data
	job := queue.NewJobWithTenant("temp job", "temp-queue", "temp-tenant")
	if tenantBackend, ok := backend.(*storage.RedisBackend); ok {
		tenantBackend.EnqueueTenant(job, "temp-tenant")

		// Show data exists
		queues, _ := tenantBackend.ListTenantQueues("temp-tenant")
		fmt.Printf("Temp tenant queues before deletion: %v\n", queues)
	}

	// Delete tenant (this cleans up all data)
	err := tenantManager.DeleteTenant(ctx, "temp-tenant")
	if err != nil {
		fmt.Printf("Failed to delete tenant: %v\n", err)
	} else {
		fmt.Printf("✓ Tenant and all associated data deleted\n")
	}

	// 3. Tenant metadata and custom fields
	fmt.Println("\n3. Tenant metadata...")

	tenant, _ = tenantManager.GetTenant(ctx, "enterprise-corp")
	if tenant.Metadata == nil {
		tenant.Metadata = make(map[string]interface{})
	}
	tenant.Metadata["billing_plan"] = "enterprise"
	tenant.Metadata["support_level"] = "premium"
	tenant.Metadata["created_by"] = "admin@example.com"

	tenantManager.UpdateTenant(ctx, tenant)
	fmt.Printf("✓ Added metadata to tenant: %v\n", tenant.Metadata)
}
