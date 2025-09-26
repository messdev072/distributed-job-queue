package api

import (
	"context"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ContextKey is a custom type for context keys to avoid collisions
type ContextKey string

const (
	UserContextKey ContextKey = "user"
)

// AuthService defines the interface for authentication and authorization
type AuthService interface {
	AuthenticateUser(ctx context.Context, token string) (*User, error)
}

// User represents an authenticated user
type User struct {
	ID       string   `json:"id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	TenantID string   `json:"tenant_id"` // Primary tenant for non-admin users
}

// AdminAPI provides HTTP endpoints for tenant management and monitoring
type AdminAPI struct {
	tenantManager queue.TenantManager
	backend       queue.Backend
	auth          AuthService
}

// AuthProvider defines authentication and authorization interface
type AuthProvider interface {
	AuthenticateUser(ctx context.Context, token string) (*User, error)
	AuthorizeAction(ctx context.Context, user *User, action string, resource string) error
}

// Permission represents an action that can be performed
type Permission string

const (
	PermissionViewTenants  Permission = "view_tenants"
	PermissionCreateTenant Permission = "create_tenant"
	PermissionUpdateTenant Permission = "update_tenant"
	PermissionDeleteTenant Permission = "delete_tenant"
	PermissionViewQueues   Permission = "view_queues"
	PermissionManageQueues Permission = "manage_queues"
	PermissionViewJobs     Permission = "view_jobs"
	PermissionManageJobs   Permission = "manage_jobs"
	PermissionViewMetrics  Permission = "view_metrics"
	PermissionSystemAdmin  Permission = "system_admin"
)

// Role represents a set of permissions
type Role struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

// Predefined roles
var (
	AdminRole = Role{
		Name: "admin",
		Permissions: []Permission{
			PermissionViewTenants, PermissionCreateTenant, PermissionUpdateTenant, PermissionDeleteTenant,
			PermissionViewQueues, PermissionManageQueues, PermissionViewJobs, PermissionManageJobs,
			PermissionViewMetrics, PermissionSystemAdmin,
		},
	}

	TenantAdminRole = Role{
		Name: "tenant_admin",
		Permissions: []Permission{
			PermissionViewQueues, PermissionManageQueues, PermissionViewJobs, PermissionManageJobs,
			PermissionViewMetrics,
		},
	}

	ViewerRole = Role{
		Name: "viewer",
		Permissions: []Permission{
			PermissionViewQueues, PermissionViewJobs, PermissionViewMetrics,
		},
	}
)

// NewAdminAPI creates a new admin API instance
func NewAdminAPI(tenantManager queue.TenantManager, backend queue.Backend, auth AuthProvider) *AdminAPI {
	return &AdminAPI{
		tenantManager: tenantManager,
		backend:       backend,
		auth:          auth,
	}
}

// SetupRoutes configures HTTP routes for the admin API
func (api *AdminAPI) SetupRoutes(mux *http.ServeMux) {
	// Tenant management
	mux.HandleFunc("/api/admin/tenants", api.authMiddleware(api.handleTenants))

	// Tenant-specific endpoints (simplified for standard library)
	mux.HandleFunc("/api/admin/tenant/", api.authMiddleware(api.handleTenantOperations))

	// System-wide endpoints
	mux.HandleFunc("/api/admin/system/health", api.authMiddleware(api.getSystemHealth))
	mux.HandleFunc("/api/admin/system/metrics", api.authMiddleware(api.getSystemMetrics))
}

// Helper function to extract path parameters
func extractPathParam(path, prefix string) string {
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	remaining := strings.TrimPrefix(path, prefix)
	if remaining == "" {
		return ""
	}
	parts := strings.Split(remaining, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// Combined handler for tenant operations
func (api *AdminAPI) handleTenants(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		api.listTenants(w, r)
	case "POST":
		api.createTenant(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Combined handler for tenant-specific operations
func (api *AdminAPI) handleTenantOperations(w http.ResponseWriter, r *http.Request) {
	// Extract tenant ID from path
	tenantID := extractPathParam(r.URL.Path, "/api/admin/tenant/")
	if tenantID == "" {
		http.Error(w, "Tenant ID required", http.StatusBadRequest)
		return
	}

	// Determine operation based on remaining path
	remaining := strings.TrimPrefix(r.URL.Path, "/api/admin/tenant/"+tenantID)

	switch {
	case remaining == "" || remaining == "/":
		// Tenant CRUD operations
		switch r.Method {
		case "GET":
			api.getTenantByParam(w, r, tenantID)
		case "PUT":
			api.updateTenantByParam(w, r, tenantID)
		case "DELETE":
			api.deleteTenantByParam(w, r, tenantID)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	case strings.HasPrefix(remaining, "/queues"):
		api.handleTenantQueues(w, r, tenantID, remaining)
	case strings.HasPrefix(remaining, "/usage"):
		api.getTenantUsageByID(w, r, tenantID)
	case strings.HasPrefix(remaining, "/stats"):
		api.getTenantStatsByID(w, r, tenantID)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

// Handle tenant queue operations
func (api *AdminAPI) handleTenantQueues(w http.ResponseWriter, r *http.Request, tenantID, path string) {
	if path == "/queues" || path == "/queues/" {
		// List all queues for tenant
		api.listTenantQueuesByID(w, r, tenantID)
		return
	}

	// Extract queue name
	queueName := extractPathParam(path, "/queues/")
	if queueName == "" {
		http.Error(w, "Queue name required", http.StatusBadRequest)
		return
	}

	remaining := strings.TrimPrefix(path, "/queues/"+queueName)
	switch {
	case remaining == "" || remaining == "/":
		api.getTenantQueueByID(w, r, tenantID, queueName)
	case strings.HasPrefix(remaining, "/jobs"):
		api.listTenantJobsByID(w, r, tenantID, queueName)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

// Middleware for authentication and authorization
func (api *AdminAPI) authMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == authHeader {
			http.Error(w, "Bearer token required", http.StatusUnauthorized)
			return
		}

		// Authenticate user
		user, err := api.auth.AuthenticateUser(r.Context(), token)
		if err != nil {
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		// Add user to request context
		ctx := context.WithValue(r.Context(), UserContextKey, user)
		r = r.WithContext(ctx)

		handler(w, r)
	}
}

// Authorization helper
func (api *AdminAPI) authorize(ctx context.Context, permission Permission, tenantID string) error {
	user := ctx.Value("user").(*User)

	// System admins can do anything
	if api.hasRole(user, "admin") {
		return nil
	}

	// For tenant-specific operations, check if user belongs to tenant
	if tenantID != "" && user.TenantID != tenantID {
		return queue.ErrUnauthorized
	}

	// Check if user has required permission
	if !api.hasPermission(user, permission) {
		return queue.ErrUnauthorized
	}

	return nil
}

func (api *AdminAPI) hasRole(user *User, roleName string) bool {
	for _, role := range user.Roles {
		if role == roleName {
			return true
		}
	}
	return false
}

func (api *AdminAPI) hasPermission(user *User, permission Permission) bool {
	for _, roleName := range user.Roles {
		var role Role
		switch roleName {
		case "admin":
			role = AdminRole
		case "tenant_admin":
			role = TenantAdminRole
		case "viewer":
			role = ViewerRole
		default:
			continue
		}

		for _, perm := range role.Permissions {
			if perm == permission {
				return true
			}
		}
	}
	return false
}

// Tenant management endpoints

func (api *AdminAPI) listTenants(w http.ResponseWriter, r *http.Request) {
	if err := api.authorize(r.Context(), PermissionViewTenants, ""); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50 // Default limit
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	tenants, err := api.tenantManager.ListTenants(r.Context(), limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenants": tenants,
		"limit":   limit,
		"offset":  offset,
	})
}

func (api *AdminAPI) createTenant(w http.ResponseWriter, r *http.Request) {
	if err := api.authorize(r.Context(), PermissionCreateTenant, ""); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var tenant queue.Tenant
	if err := json.NewDecoder(r.Body).Decode(&tenant); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Set creation timestamp
	tenant.CreatedAt = time.Now()
	tenant.UpdatedAt = time.Now()
	tenant.Status = queue.TenantStatusActive

	// Set default quotas if not provided
	if tenant.Quotas.MaxQueueLength == 0 {
		tenant.Quotas = queue.DefaultTenantQuotas
	}
	if tenant.RateLimit.QueueRatePerSecond == 0 {
		tenant.RateLimit = queue.DefaultTenantRateLimit
	}

	if err := api.tenantManager.CreateTenant(r.Context(), &tenant); err != nil {
		if err == queue.ErrTenantExists {
			http.Error(w, err.Error(), http.StatusConflict)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tenant)
}

func (api *AdminAPI) getTenantByParam(w http.ResponseWriter, r *http.Request, tenantID string) {
	if err := api.authorize(r.Context(), PermissionViewTenants, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	tenant, err := api.tenantManager.GetTenant(r.Context(), tenantID)
	if err != nil {
		if err == queue.ErrTenantNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenant)
}

func (api *AdminAPI) updateTenantByParam(w http.ResponseWriter, r *http.Request, tenantID string) {
	if err := api.authorize(r.Context(), PermissionUpdateTenant, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var tenant queue.Tenant
	if err := json.NewDecoder(r.Body).Decode(&tenant); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Ensure tenant ID matches URL
	tenant.ID = tenantID
	tenant.UpdatedAt = time.Now()

	if err := api.tenantManager.UpdateTenant(r.Context(), &tenant); err != nil {
		if err == queue.ErrTenantNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenant)
}

func (api *AdminAPI) deleteTenantByParam(w http.ResponseWriter, r *http.Request, tenantID string) {
	if err := api.authorize(r.Context(), PermissionDeleteTenant, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	if err := api.tenantManager.DeleteTenant(r.Context(), tenantID); err != nil {
		if err == queue.ErrTenantNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Missing method implementations for parameter-based routing

func (api *AdminAPI) getTenantUsageByID(w http.ResponseWriter, r *http.Request, tenantID string) {
	if err := api.authorize(r.Context(), PermissionViewMetrics, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	usage, err := api.tenantManager.GetTenantUsage(r.Context(), tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(usage)
}

func (api *AdminAPI) getTenantStatsByID(w http.ResponseWriter, r *http.Request, tenantID string) {
	if err := api.authorize(r.Context(), PermissionViewMetrics, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Get basic tenant info and usage
	tenant, err := api.tenantManager.GetTenant(r.Context(), tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	usage, err := api.tenantManager.GetTenantUsage(r.Context(), tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats := map[string]interface{}{
		"tenant": tenant,
		"usage":  usage,
		"quota_usage": map[string]interface{}{
			"queue_length_pct":  float64(usage.TotalQueueLength) / float64(tenant.Quotas.MaxQueueLength) * 100,
			"active_queues_pct": float64(usage.ActiveQueues) / float64(tenant.Quotas.MaxQueues) * 100,
			"daily_jobs_pct":    float64(usage.JobsToday) / float64(tenant.Quotas.MaxJobsPerDay) * 100,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (api *AdminAPI) listTenantQueuesByID(w http.ResponseWriter, r *http.Request, tenantID string) {
	if err := api.authorize(r.Context(), PermissionViewQueues, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	backend, ok := api.backend.(interface {
		ListTenantQueues(tenantID string) ([]string, error)
	})
	if !ok {
		http.Error(w, "Backend does not support tenant operations", http.StatusNotImplemented)
		return
	}

	queues, err := backend.ListTenantQueues(tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get queue lengths
	queueInfo := make([]map[string]interface{}, len(queues))
	for i, queueName := range queues {
		length, _ := backend.(interface {
			GetTenantQueueLength(queueName string, tenantID string) (int64, error)
		}).GetTenantQueueLength(queueName, tenantID)

		queueInfo[i] = map[string]interface{}{
			"name":   queueName,
			"length": length,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenant_id": tenantID,
		"queues":    queueInfo,
	})
}

func (api *AdminAPI) getTenantQueueByID(w http.ResponseWriter, r *http.Request, tenantID, queueName string) {
	if err := api.authorize(r.Context(), PermissionViewQueues, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	backend, ok := api.backend.(interface {
		GetTenantQueueLength(queueName string, tenantID string) (int64, error)
	})
	if !ok {
		http.Error(w, "Backend does not support tenant operations", http.StatusNotImplemented)
		return
	}

	length, err := backend.GetTenantQueueLength(queueName, tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenant_id":  tenantID,
		"queue_name": queueName,
		"length":     length,
	})
}

func (api *AdminAPI) listTenantJobsByID(w http.ResponseWriter, r *http.Request, tenantID, queueName string) {
	if err := api.authorize(r.Context(), PermissionViewJobs, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// This would require additional backend methods to list jobs
	// For now, return basic info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenant_id":  tenantID,
		"queue_name": queueName,
		"message":    "Job listing not yet implemented",
	})
}

// Queue management endpoints

func (api *AdminAPI) listTenantQueues(w http.ResponseWriter, r *http.Request) {
	// Extract tenant ID from URL path
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 {
		http.Error(w, "Invalid URL path", http.StatusBadRequest)
		return
	}
	tenantID := pathParts[3] // /api/tenants/{tenantId}/queues

	if err := api.authorize(r.Context(), PermissionViewQueues, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	backend, ok := api.backend.(interface {
		ListTenantQueues(tenantID string) ([]string, error)
	})
	if !ok {
		http.Error(w, "Backend does not support tenant operations", http.StatusNotImplemented)
		return
	}

	queues, err := backend.ListTenantQueues(tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get queue lengths
	queueInfo := make([]map[string]interface{}, len(queues))
	for i, queueName := range queues {
		length, _ := backend.(interface {
			GetTenantQueueLength(queueName string, tenantID string) (int64, error)
		}).GetTenantQueueLength(queueName, tenantID)

		queueInfo[i] = map[string]interface{}{
			"name":   queueName,
			"length": length,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenant_id": tenantID,
		"queues":    queueInfo,
	})
}

func (api *AdminAPI) getTenantQueue(w http.ResponseWriter, r *http.Request) {
	// Extract tenant ID and queue name from URL path
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 6 {
		http.Error(w, "Invalid URL path", http.StatusBadRequest)
		return
	}
	tenantID := pathParts[3] // /api/tenants/{tenantId}/queues/{queueName}
	queueName := pathParts[5]

	if err := api.authorize(r.Context(), PermissionViewQueues, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	backend, ok := api.backend.(interface {
		GetTenantQueueLength(queueName string, tenantID string) (int64, error)
	})
	if !ok {
		http.Error(w, "Backend does not support tenant operations", http.StatusNotImplemented)
		return
	}

	length, err := backend.GetTenantQueueLength(queueName, tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenant_id":  tenantID,
		"queue_name": queueName,
		"length":     length,
	})
}

func (api *AdminAPI) listTenantJobs(w http.ResponseWriter, r *http.Request) {
	// Extract tenant ID and queue name from URL path
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 7 {
		http.Error(w, "Invalid URL path", http.StatusBadRequest)
		return
	}
	tenantID := pathParts[3] // /api/tenants/{tenantId}/queues/{queueName}/jobs
	queueName := pathParts[5]

	if err := api.authorize(r.Context(), PermissionViewJobs, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// This would require additional backend methods to list jobs
	// For now, return basic info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tenant_id":  tenantID,
		"queue_name": queueName,
		"message":    "Job listing not yet implemented",
	})
}

// Usage and metrics endpoints

func (api *AdminAPI) getTenantUsage(w http.ResponseWriter, r *http.Request) {
	// Extract tenant ID from URL path
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 5 {
		http.Error(w, "Invalid URL path", http.StatusBadRequest)
		return
	}
	tenantID := pathParts[3] // /api/tenants/{tenantId}/usage

	if err := api.authorize(r.Context(), PermissionViewMetrics, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	usage, err := api.tenantManager.GetTenantUsage(r.Context(), tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(usage)
}

func (api *AdminAPI) getTenantStats(w http.ResponseWriter, r *http.Request) {
	// Extract tenant ID from URL path
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 5 {
		http.Error(w, "Invalid URL path", http.StatusBadRequest)
		return
	}
	tenantID := pathParts[3] // /api/tenants/{tenantId}/stats

	if err := api.authorize(r.Context(), PermissionViewMetrics, tenantID); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Get basic tenant info and usage
	tenant, err := api.tenantManager.GetTenant(r.Context(), tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	usage, err := api.tenantManager.GetTenantUsage(r.Context(), tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats := map[string]interface{}{
		"tenant": tenant,
		"usage":  usage,
		"quota_usage": map[string]interface{}{
			"queue_length_pct":  float64(usage.TotalQueueLength) / float64(tenant.Quotas.MaxQueueLength) * 100,
			"active_queues_pct": float64(usage.ActiveQueues) / float64(tenant.Quotas.MaxQueues) * 100,
			"daily_jobs_pct":    float64(usage.JobsToday) / float64(tenant.Quotas.MaxJobsPerDay) * 100,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// System endpoints

func (api *AdminAPI) getSystemHealth(w http.ResponseWriter, r *http.Request) {
	if err := api.authorize(r.Context(), PermissionSystemAdmin, ""); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

func (api *AdminAPI) getSystemMetrics(w http.ResponseWriter, r *http.Request) {
	if err := api.authorize(r.Context(), PermissionSystemAdmin, ""); err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Get system-wide metrics
	metrics := map[string]interface{}{
		"timestamp":   time.Now(),
		"system_info": "System metrics would be implemented here",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}
