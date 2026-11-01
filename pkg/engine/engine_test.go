package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

func TestEngineWorkerExecution(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available on localhost:6379")
	}

	ns := "test-engine-" + uuid.New().String()[:8]
	q := queue.NewRedisQueue(rdb, ns)
	defer q.Close()

	cfg := DefaultServerConfig()
	cfg.Concurrency = 2
	cfg.PollInterval = 10 * time.Millisecond
	cfg.EmptyQueueDelay = 20 * time.Millisecond
	cfg.EnableScheduler = false

	srv := NewServer(q, cfg)

	var executed atomic.Int32
	srv.RegisterFunc("test:job", func(ctx context.Context, task *types.Task) error {
		executed.Add(1)
		return nil
	})

	// Enqueue 3 jobs
	for i := 0; i < 3; i++ {
		task, _ := types.NewTask("test:job", map[string]int{"idx": i})
		_ = q.Enqueue(context.Background(), task)
	}

	runCtx, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	go func() {
		_ = srv.Start(runCtx)
		close(serverDone)
	}()

	// Wait for jobs to be processed
	for i := 0; i < 50; i++ {
		if executed.Load() == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	stopServer()
	srv.Stop()
	<-serverDone

	if executed.Load() != 3 {
		t.Errorf("expected 3 jobs executed, got %d", executed.Load())
	}
}

func TestEnginePanicSafety(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available on localhost:6379")
	}

	ns := "test-panic-" + uuid.New().String()[:8]
	q := queue.NewRedisQueue(rdb, ns)
	defer q.Close()

	cfg := DefaultServerConfig()
	cfg.Concurrency = 1
	cfg.PollInterval = 10 * time.Millisecond
	cfg.EmptyQueueDelay = 20 * time.Millisecond
	cfg.EnableScheduler = false

	srv := NewServer(q, cfg)

	// Handler that intentionally panics
	srv.RegisterFunc("panic:job", func(ctx context.Context, task *types.Task) error {
		panic("simulated catastrophic panic inside business logic")
	})

	task, _ := types.NewTask("panic:job", "boom", types.WithMaxRetries(1))
	_ = q.Enqueue(context.Background(), task)

	runCtx, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan struct{})
	go func() {
		_ = srv.Start(runCtx)
		close(serverDone)
	}()

	// Give worker time to process and catch the panic
	time.Sleep(200 * time.Millisecond)
	stopServer()
	srv.Stop()
	<-serverDone

	// Verify task was marked as failed / DLQ rather than hanging
	saved, err := q.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("failed to get task: %v", err)
	}

	if saved.State != types.StateDead {
		t.Errorf("expected task state to be dead after panic, got %s", saved.State)
	}
}
