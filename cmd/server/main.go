package main

import (
	"distributed-job-queue/pkg/api"
	"distributed-job-queue/pkg/logging"
	"distributed-job-queue/pkg/storage"
	"log"
	"net/http"

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
	// Prometheus metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/workers", s.ListWorkersHandler)

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
