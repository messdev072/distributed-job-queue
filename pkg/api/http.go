package api

import (
	"distributed-job-queue/pkg/logging"
	"distributed-job-queue/pkg/queue"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

type Server struct {
	Q queue.Queue
}

func (s *Server) EnqueueHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Queue    string `json:"queue"`
		Payload  string `json:"payload"`
		Priority int    `json:"priority"`
		DelaySeconds int `json:"delay_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		logging.L().Error("enqueue decode error", zap.Error(err), zap.String("job_id", ""), zap.String("queue", body.Queue), zap.String("worker_id", ""))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job := queue.NewJob(body.Payload, body.Queue)
	job.Priority = body.Priority
	if body.DelaySeconds > 0 {
		job.AvailableAt = time.Now().Add(time.Duration(body.DelaySeconds) * time.Second)
	}
	if err := s.Q.Enqueue(job); err != nil {
		logging.L().Error("enqueue failed", zap.Error(err), zap.String("job_id", job.ID), zap.String("queue", body.Queue), zap.String("worker_id", ""))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logging.L().Info("enqueued job", zap.String("job_id", job.ID), zap.String("queue", body.Queue), zap.String("worker_id", ""))

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
		logging.L().Error("get job not found", zap.Error(err), zap.String("job_id", id), zap.String("queue", ""), zap.String("worker_id", ""))
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	logging.L().Info("get job", zap.String("job_id", job.ID), zap.String("queue", job.QueueName), zap.String("worker_id", ""))

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
	logging.L().Info("list workers", zap.Int("count", len(workers)), zap.String("job_id", ""), zap.String("queue", ""), zap.String("worker_id", ""))
	json.NewEncoder(w).Encode(workers)
}

// GET /queues -> show queue lengths for all queues
func (s *Server) ListQueuesHandler(w http.ResponseWriter, r *http.Request) {
	resp := map[string]int64{}
	queues, err := s.Q.Client().SMembers(s.Q.Ctx(), "queues").Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, q := range queues {
		key := fmt.Sprintf("jobs:%s:z", q)
		if n, err := s.Q.Client().ZCard(s.Q.Ctx(), key).Result(); err == nil {
			resp[q] = n
		}
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// GET /jobs/:id -> detailed job info
func (s *Server) GetJobDetailHandler(w http.ResponseWriter, r *http.Request) {
	id := getJobIDFromPath(r.URL.Path)
	if id == "" {
		http.Error(w, "missing job ID", http.StatusBadRequest)
		return
	}
	job, err := s.Q.GetJob(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// fetch and parse limited history
	rawEvents, _ := s.Q.Client().LRange(s.Q.Ctx(), fmt.Sprintf("job:%s:events", id), 0, 50).Result()
	type Event struct {
		Type      string `json:"type"`
		Queue     string `json:"queue"`
		Timestamp int64  `json:"timestamp"`
	}
	parsed := make([]Event, 0, len(rawEvents))
	for _, e := range rawEvents {
		var ev Event
		if err := json.Unmarshal([]byte(e), &ev); err == nil {
			parsed = append(parsed, ev)
		}
	}
	resp := map[string]interface{}{
		"job":    job,
		"events": parsed,
	}
	json.NewEncoder(w).Encode(resp)
}

// POST /jobs/:id/retry -> manually requeue a failed job
func (s *Server) RetryJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := getJobIDFromPath(r.URL.Path)
	if id == "" {
		http.Error(w, "missing job ID", http.StatusBadRequest)
		return
	}
	job, err := s.Q.GetJob(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Only requeue if failed/canceled
	if job.Status != queue.StatusFailed && job.Status != queue.StatusCanceled {
		http.Error(w, "job not in failed/canceled state", http.StatusBadRequest)
		return
	}
	job.Status = queue.StatusPending
	job.UpdatedAt = time.Now()
	job.RetryCount = 0
	_ = s.Q.UpdateJob(job)
	_ = s.Q.Client().LPush(s.Q.Ctx(), fmt.Sprintf("jobs:%s", job.QueueName), job.ID).Err()
	w.WriteHeader(http.StatusAccepted)
}

// POST /jobs/:id/cancel -> mark job canceled
func (s *Server) CancelJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := getJobIDFromPath(r.URL.Path)
	if id == "" {
		http.Error(w, "missing job ID", http.StatusBadRequest)
		return
	}
	job, err := s.Q.GetJob(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	job.Status = queue.StatusCanceled
	job.UpdatedAt = time.Now()
	if err := s.Q.UpdateJob(job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// getJobIDFromPath returns the job ID from URLs like /jobs/:id or /admin/jobs/:id/(action)
func getJobIDFromPath(path string) string {
	parts := strings.Split(path, "/")
	segs := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			segs = append(segs, p)
		}
	}
	if len(segs) == 0 {
		return ""
	}
	last := segs[len(segs)-1]
	if last == "retry" || last == "cancel" {
		if len(segs) >= 2 {
			return segs[len(segs)-2]
		}
		return ""
	}
	return last
}
