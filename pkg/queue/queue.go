package queue

import (
	"context"
	"time"

	"github.com/pulsequeue/pulse-queue/pkg/types"
)

// WorkerHeartbeat encapsulates dynamic state reported by active worker nodes.
type WorkerHeartbeat struct {
	WorkerID     string    `json:"worker_id"`
	Concurrency  int       `json:"concurrency"`
	ActiveJobs   int       `json:"active_jobs"`
	StartedAt    time.Time `json:"started_at"`
	LastSeen     time.Time `json:"last_seen"`
	Hostname     string    `json:"hostname"`
}

// QueueStats captures queue depth, latency, and status across all priorities.
type QueueStats struct {
	Critical   int64 `json:"critical"`
	High       int64 `json:"high"`
	Default    int64 `json:"default"`
	Low        int64 `json:"low"`
	Delayed    int64 `json:"delayed"`
	Processing int64 `json:"processing"`
	DLQ        int64 `json:"dlq"`
	Completed  int64 `json:"completed"`
	Workers    int   `json:"workers"`
}

// Queue defines the contract for task queue storage backends.
type Queue interface {
	Enqueue(ctx context.Context, task *types.Task) error
	Dequeue(ctx context.Context, workerID string) (*types.Task, error)
	Ack(ctx context.Context, task *types.Task, result []byte) error
	Nack(ctx context.Context, task *types.Task, taskErr error) error
	GetTask(ctx context.Context, taskID string) (*types.Task, error)
	MigrateDelayed(ctx context.Context, batchSize int) (int, error)
	RequeueOrphans(ctx context.Context, batchSize int) (int, error)
	RegisterHeartbeat(ctx context.Context, workerID string, hb WorkerHeartbeat) error
	GetWorkers(ctx context.Context) ([]WorkerHeartbeat, error)
	GetStats(ctx context.Context) (*QueueStats, error)
	GetDLQ(ctx context.Context, start, stop int64) ([]*types.Task, int64, error)
	RetryDLQTask(ctx context.Context, taskID string) error
	Close() error
}
