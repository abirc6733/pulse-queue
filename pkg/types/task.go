package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskState represents the lifecycle status of a task.
type TaskState string

const (
	StatePending    TaskState = "pending"    // Waiting in queue to be picked up
	StateScheduled  TaskState = "scheduled"  // Delayed / scheduled for future execution
	StateActive     TaskState = "active"     // Currently being processed by a worker
	StateCompleted  TaskState = "completed"  // Executed successfully
	StateRetry      TaskState = "retry"      // Failed, waiting for backoff retry
	StateDead       TaskState = "dead"       // Failed and exhausted all retries (sent to DLQ)
)

// Priority levels for task queues.
type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityDefault  Priority = "default"
	PriorityLow      Priority = "low"
)

// AllPriorities defines the dequeue order from highest to lowest.
var AllPriorities = []Priority{
	PriorityCritical,
	PriorityHigh,
	PriorityDefault,
	PriorityLow,
}

// Task represents a unit of background work.
type Task struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    Priority        `json:"priority"`
	State       TaskState       `json:"state"`
	MaxRetries  int             `json:"max_retries"`
	RetryCount  int             `json:"retry_count"`
	Timeout     time.Duration   `json:"timeout"`
	UniqueKey   string          `json:"unique_key,omitempty"`
	UniqueTTL   time.Duration   `json:"unique_ttl,omitempty"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
}

// NewTask creates a new Task instance with sensible defaults.
func NewTask(typeName string, payload any, opts ...Option) (*Task, error) {
	if typeName == "" {
		return nil, fmt.Errorf("task type cannot be empty")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal task payload: %w", err)
	}

	task := &Task{
		ID:          uuid.New().String(),
		Type:        typeName,
		Payload:     payloadBytes,
		Priority:    PriorityDefault,
		State:       StatePending,
		MaxRetries:  3,
		RetryCount:  0,
		Timeout:     30 * time.Second,
		ScheduledAt: time.Now().UTC(),
		CreatedAt:   time.Now().UTC(),
	}

	for _, opt := range opts {
		opt(task)
	}

	if task.ScheduledAt.After(time.Now().UTC()) {
		task.State = StateScheduled
	}

	return task, nil
}

// UnmarshalPayload helper to decode payload into target struct.
func (t *Task) UnmarshalPayload(v any) error {
	return json.Unmarshal(t.Payload, v)
}
