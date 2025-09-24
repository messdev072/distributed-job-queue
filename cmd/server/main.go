package main

import (
	"distributed-job-queue/pkg/api"
	"distributed-job-queue/pkg/logging"
	"distributed-job-queue/pkg/storage"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	_ = logging.Init(true)
	q := storage.NewRedisQueue("localhost:6379", "jobs")

	s := &api.Server{Q: q}

	http.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			s.EnqueueHandler(w, r)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})
	http.HandleFunc("/jobs/", s.GetJobHandler)
	// Admin endpoints
	http.HandleFunc("/queues", s.ListQueuesHandler)
	// Admin: protect with simple bearer token from ADMIN_TOKEN
	admin := func(h http.HandlerFunc) http.HandlerFunc {
		token := os.Getenv("ADMIN_TOKEN")
		return func(w http.ResponseWriter, r *http.Request) {
			if token == "" {
				http.Error(w, "admin disabled", http.StatusForbidden)
				return
			}
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			h(w, r)
		}
	}
	http.HandleFunc("/admin/jobs/", admin(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/retry") {
			s.RetryJobHandler(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			s.CancelJobHandler(w, r)
			return
		}
		// default to job detail if not matched by above
		s.GetJobDetailHandler(w, r)
	}))
	// Prometheus metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/workers", s.ListWorkersHandler)

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
