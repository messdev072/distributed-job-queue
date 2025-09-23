package main

import (
	"distributed-job-queue/pkg/api"
	"distributed-job-queue/pkg/storage"
	"log"
	"net/http"
)

func main() {
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

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
