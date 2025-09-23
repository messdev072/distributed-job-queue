package api

import (
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"net/http"
	"strings"
)

type Server struct {
	Q queue.Queue
}

func (s *Server) EnqueueHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Queue   string `json:"queue"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job := queue.NewJob(body.Payload, body.Queue)
	if err := s.Q.Enqueue(job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(job)
}

func (s *Server) GetJobHandler(w http.ResponseWriter, r *http.Request) {
	// Expecting URL like /jobs/<id>
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "missing job ID", http.StatusBadRequest)
		return
	}
	id := parts[2]

	job, err := s.Q.GetJob(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(job)
}

func (s *Server) ListWorkersHandler(w http.ResponseWriter, r *http.Request) {
	keys, _ := s.Q.Client().Keys(s.Q.Ctx(), "worker:*").Result()
	workers := []map[string]string{}
	for _, k := range keys {
		data, _ := s.Q.Client().HGetAll(s.Q.Ctx(), k).Result()
		workers = append(workers, data)
	}
	json.NewEncoder(w).Encode(workers)
}
