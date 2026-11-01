package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/abirc6733/pulse-queue/pkg/queue"
	"github.com/abirc6733/pulse-queue/pkg/types"
)

// ServerConfig configures the worker execution daemon.
type ServerConfig struct {
	Concurrency     int
	WorkerID        string
	PollInterval    time.Duration
	EmptyQueueDelay time.Duration
	ShutdownTimeout time.Duration
	EnableScheduler bool
}

// DefaultServerConfig provides robust production defaults.
func DefaultServerConfig() ServerConfig {
	hostname, _ := os.Hostname()
	return ServerConfig{
		Concurrency:     10,
		WorkerID:        fmt.Sprintf("worker-%s-%s", hostname, uuid.New().String()[:8]),
		PollInterval:    20 * time.Millisecond,
		EmptyQueueDelay: 400 * time.Millisecond,
		ShutdownTimeout: 15 * time.Second,
		EnableScheduler: true,
	}
}

// Server coordinates task consumption, concurrency management, and lifecycle.
type Server struct {
	q        queue.Queue
	cfg      ServerConfig
	registry *HandlerRegistry

	activeJobs atomic.Int32
	startedAt  time.Time
	stopCh     chan struct{}
	wg         sync.WaitGroup
	running    atomic.Bool
}

// NewServer initializes a new worker daemon.
func NewServer(q queue.Queue, cfg ServerConfig) *Server {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = uuid.New().String()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}

	return &Server{
		q:        q,
		cfg:      cfg,
		registry: NewHandlerRegistry(),
		stopCh:   make(chan struct{}),
	}
}

// Register registers a handler for a task type.
func (s *Server) Register(taskType string, h Handler) {
	s.registry.Register(taskType, h)
}

// RegisterFunc registers a simple handler function for a task type.
func (s *Server) RegisterFunc(taskType string, fn HandlerFunc) {
	s.registry.RegisterFunc(taskType, fn)
}

// Start launches the worker engine and blocks until context cancellation or Stop().
func (s *Server) Start(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("server is already running")
	}

	s.startedAt = time.Now().UTC()
	log.Printf("[PulseQueue] Starting worker '%s' with concurrency=%d", s.cfg.WorkerID, s.cfg.Concurrency)

	// Heartbeat ticker
	s.wg.Add(1)
	go s.heartbeatLoop()

	// Scheduler & orphan reaper loop
	if s.cfg.EnableScheduler {
		s.wg.Add(1)
		go s.schedulerLoop()
	}

	// Worker dispatch loop with semaphore concurrency control
	semaphore := make(chan struct{}, s.cfg.Concurrency)

	for {
		select {
		case <-s.stopCh:
			log.Printf("[PulseQueue] Worker '%s' received stop signal, draining active jobs...", s.cfg.WorkerID)
			return s.waitShutdown()
		case <-ctx.Done():
			log.Printf("[PulseQueue] Context canceled, shutting down...")
			return s.waitShutdown()
		default:
		}

		// Acquire slot from concurrency semaphore
		select {
		case semaphore <- struct{}{}:
		case <-s.stopCh:
			return s.waitShutdown()
		case <-ctx.Done():
			return s.waitShutdown()
		}

		task, err := s.q.Dequeue(ctx, s.cfg.WorkerID)
		if err != nil {
			log.Printf("[PulseQueue] Dequeue error: %v", err)
			<-semaphore
			time.Sleep(s.cfg.EmptyQueueDelay)
			continue
		}

		if task == nil {
			// No task available, release slot and back off slightly
			<-semaphore
			time.Sleep(s.cfg.EmptyQueueDelay)
			continue
		}

		// Dispatch task to goroutine
		s.wg.Add(1)
		s.activeJobs.Add(1)

		go func(t *types.Task) {
			defer func() {
				<-semaphore
				s.activeJobs.Add(-1)
				s.wg.Done()
			}()

			s.executeTask(ctx, t)
		}(task)
	}
}

// executeTask runs a single task with panic recovery and timeout enforcement.
func (s *Server) executeTask(parentCtx context.Context, task *types.Task) {
	handler, err := s.registry.Get(task.Type)
	if err != nil {
		log.Printf("[PulseQueue] Unknown task type '%s' (task_id=%s): %v", task.Type, task.ID, err)
		_ = s.q.Nack(parentCtx, task, err)
		return
	}

	timeout := task.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	taskCtx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	var (
		res []byte
		execErr error
	)

	// Panic protection
	runDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("task panicked: %v", r)
			}
			close(runDone)
		}()
		res, execErr = handler(taskCtx, task)
	}()

	select {
	case <-runDone:
		if execErr != nil {
			log.Printf("[PulseQueue] Task %s failed: %v", task.ID, execErr)
			_ = s.q.Nack(parentCtx, task, execErr)
		} else {
			_ = s.q.Ack(parentCtx, task, res)
		}
	case <-taskCtx.Done():
		timeoutErr := fmt.Errorf("task exceeded timeout (%v): %w", timeout, taskCtx.Err())
		log.Printf("[PulseQueue] Task %s timed out", task.ID)
		_ = s.q.Nack(parentCtx, task, timeoutErr)
	}
}

func (s *Server) heartbeatLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	hostname, _ := os.Hostname()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			hb := queue.WorkerHeartbeat{
				WorkerID:    s.cfg.WorkerID,
				Concurrency: s.cfg.Concurrency,
				ActiveJobs:  int(s.activeJobs.Load()),
				StartedAt:   s.startedAt,
				LastSeen:    time.Now().UTC(),
				Hostname:    hostname,
			}
			_ = s.q.RegisterHeartbeat(context.Background(), s.cfg.WorkerID, hb)
		}
	}
}

func (s *Server) schedulerLoop() {
	defer s.wg.Done()
	migrateTicker := time.NewTicker(1 * time.Second)
	reaperTicker := time.NewTicker(10 * time.Second)
	defer migrateTicker.Stop()
	defer reaperTicker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-migrateTicker.C:
			// Migrate ready delayed tasks to active queues
			_, _ = s.q.MigrateDelayed(context.Background(), 100)
		case <-reaperTicker.C:
			// Recover crashed worker orphaned tasks
			reaped, _ := s.q.RequeueOrphans(context.Background(), 50)
			if reaped > 0 {
				log.Printf("[PulseQueue] Reclaimed %d orphaned tasks from dead workers", reaped)
			}
		}
	}
}

// Stop triggers graceful shutdown.
func (s *Server) Stop() {
	if s.running.CompareAndSwap(true, false) {
		close(s.stopCh)
	}
}

func (s *Server) waitShutdown() error {
	shutdownDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		log.Printf("[PulseQueue] Worker '%s' gracefully stopped.", s.cfg.WorkerID)
		return nil
	case <-time.After(s.cfg.ShutdownTimeout):
		return fmt.Errorf("graceful shutdown timed out after %v", s.cfg.ShutdownTimeout)
	}
}
