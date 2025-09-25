package main

import (
	"distributed-job-queue/pkg/api"
	"distributed-job-queue/pkg/logging"
	"distributed-job-queue/pkg/persistence"
	"distributed-job-queue/pkg/queue"
	"distributed-job-queue/pkg/scheduler"
	"distributed-job-queue/pkg/storage"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	_ = logging.Init(true)
	q := storage.NewRedisQueue("localhost:6379", "jobs")

	// Optional Postgres persistence for lifecycle events
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		if ph, err := persistence.NewPostgresHooks(dsn); err == nil {
			q.WithHooks(ph)
			log.Println("Postgres persistence enabled for job events")
		} else {
			log.Printf("Postgres hooks init failed: %v\n", err)
		}
	}

	// Delayed job promotion ticker
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// List queues and promote
			queues, err := q.Client().SMembers(q.Ctx(), "queues").Result()
			if err != nil {
				continue
			}
			for _, name := range queues {
				_ = q.PromoteDelayed(name)
			}
		}
	}()

	schedStore := scheduler.NewStore(q.Client(), q.Ctx())
	s := &api.Server{Q: q, Schedules: schedStore}

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
	// Schedule CRUD
	http.HandleFunc("/admin/schedules", admin(s.SchedulesHandler))
	http.HandleFunc("/admin/schedules/", admin(s.SchedulesHandler))
	// Prometheus metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/workers", s.ListWorkersHandler)

	// Cron evaluation loop
	go func() {
		cronTicker := time.NewTicker(1 * time.Second)
		defer cronTicker.Stop()
		for range cronTicker.C {
			due, err := schedStore.Due(time.Now(), 0)
			if err != nil {
				continue
			}
			for _, sc := range due {
				job := queue.NewJob(sc.Payload, sc.Queue)
				job.Priority = sc.Priority
				job.RecurrenceID = sc.ID
				if err := q.Enqueue(job); err != nil {
					continue
				}
				_ = schedStore.MarkRun(sc)
			}
		}
	}()

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
