package web

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/pkg/types"
)

//go:embed static/index.html
var indexHTML []byte

// DashboardServer serves the web UI and management REST API.
type DashboardServer struct {
	q    queue.Queue
	port int
}

func NewDashboardServer(q queue.Queue, port int) *DashboardServer {
	if port <= 0 {
		port = 8080
	}
	return &DashboardServer{q: q, port: port}
}

func (s *DashboardServer) Handler() http.Handler {
	mux := http.NewServeMux()

	// API Routes
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/workers", s.handleWorkers)
	mux.HandleFunc("/api/dlq", s.handleDLQ)
	mux.HandleFunc("/api/dlq/retry", s.handleRetryDLQ)
	mux.HandleFunc("/api/enqueue", s.handleEnqueue)

	// UI
	mux.HandleFunc("/", s.handleIndex)

	return mux
}

func (s *DashboardServer) ListenAndServe() error {
	addr := fmt.Sprintf(":%d", s.port)
	return http.ListenAndServe(addr, s.Handler())
}

func (s *DashboardServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *DashboardServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.q.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, stats)
}

func (s *DashboardServer) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.q.GetWorkers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, workers)
}

func (s *DashboardServer) handleDLQ(w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	stopStr := r.URL.Query().Get("stop")
	start, _ := strconv.ParseInt(startStr, 10, 64)
	stop, _ := strconv.ParseInt(stopStr, 10, 64)
	if stop == 0 {
		stop = 50
	}

	tasks, total, err := s.q.GetDLQ(r.Context(), start, stop)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"total": total,
		"tasks": tasks,
	})
}

func (s *DashboardServer) handleRetryDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	if err := s.q.RetryDLQTask(r.Context(), taskID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{"status": "requeued", "task_id": taskID})
}

func (s *DashboardServer) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Type       string `json:"type"`
		Payload    any    `json:"payload"`
		Priority   string `json:"priority"`
		DelaySec   int    `json:"delay_sec"`
		MaxRetries int    `json:"max_retries"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var opts []types.Option
	if req.Priority != "" {
		opts = append(opts, types.WithPriority(types.Priority(req.Priority)))
	}
	if req.DelaySec > 0 {
		opts = append(opts, types.WithDelay(time.Duration(req.DelaySec)*time.Second))
	}
	if req.MaxRetries > 0 {
		opts = append(opts, types.WithMaxRetries(req.MaxRetries))
	}

	task, err := types.NewTask(req.Type, req.Payload, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.q.Enqueue(r.Context(), task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"status": "enqueued", "task": task})
}

func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

func (s *DashboardServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}
