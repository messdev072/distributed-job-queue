package tests

import (
	"bytes"
	"distributed-job-queue/pkg/api"
	"distributed-job-queue/pkg/storage"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestAdminQueuesAndJobDetail(t *testing.T) {
	// set admin token
	os.Setenv("ADMIN_TOKEN", "secret")
	q := storage.NewRedisQueue("localhost:6379", "admin_test")
	s := &api.Server{Q: q}

	// enqueue a job
	body := map[string]string{"queue": "admin_test", "payload": "x"}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(b))
	w := httptest.NewRecorder()
	s.EnqueueHandler(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	var jobResp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &jobResp)
	id := jobResp["id"].(string)

	// list queues
	req2 := httptest.NewRequest(http.MethodGet, "/queues", nil)
	w2 := httptest.NewRecorder()
	s.ListQueuesHandler(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("queues expected 200, got %d", w2.Code)
	}

	// admin job detail
	req3 := httptest.NewRequest(http.MethodGet, "/admin/jobs/"+id, nil)
	req3.Header.Set("Authorization", "Bearer secret")
	w3 := httptest.NewRecorder()
	s.GetJobDetailHandler(w3, req3)
	if w3.Code != http.StatusOK {
		t.Fatalf("job detail expected 200, got %d", w3.Code)
	}
}

func TestAdminRetryAndCancel(t *testing.T) {
	os.Setenv("ADMIN_TOKEN", "secret")
	q := storage.NewRedisQueue("localhost:6379", "admin_test2")
	s := &api.Server{Q: q}

	// create a failed job by direct enqueue + manual status flip
	// (unit test level; we just mark it failed and update)
	w := httptest.NewRecorder()
	body := map[string]string{"queue": "admin_test2", "payload": "y"}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(b))
	s.EnqueueHandler(w, req)
	var jobResp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &jobResp)
	id := jobResp["id"].(string)

	job, _ := q.GetJob(id)
	job.Status = "FAILED"
	_ = q.UpdateJob(job)

	// retry
	reqR := httptest.NewRequest(http.MethodPost, "/admin/jobs/"+id+"/retry", nil)
	reqR.Header.Set("Authorization", "Bearer secret")
	wR := httptest.NewRecorder()
	s.RetryJobHandler(wR, reqR)
	if wR.Code != http.StatusAccepted {
		t.Fatalf("retry expected 202, got %d", wR.Code)
	}

	// cancel
	reqC := httptest.NewRequest(http.MethodPost, "/admin/jobs/"+id+"/cancel", nil)
	reqC.Header.Set("Authorization", "Bearer secret")
	wC := httptest.NewRecorder()
	s.CancelJobHandler(wC, reqC)
	if wC.Code != http.StatusAccepted {
		t.Fatalf("cancel expected 202, got %d", wC.Code)
	}
}
