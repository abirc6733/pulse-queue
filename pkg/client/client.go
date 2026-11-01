package client

import (
	"context"

	"github.com/abirc6733/pulse-queue/pkg/queue"
	"github.com/abirc6733/pulse-queue/pkg/types"
)

// Client provides a clean, ergonomic API for producers to enqueue and monitor background tasks.
type Client struct {
	q queue.Queue
}

// NewClient creates a new Task Queue Client.
func NewClient(q queue.Queue) *Client {
	return &Client{q: q}
}

// Enqueue constructs and enqueues a new background task.
func (c *Client) Enqueue(ctx context.Context, taskType string, payload any, opts ...types.Option) (*types.Task, error) {
	task, err := types.NewTask(taskType, payload, opts...)
	if err != nil {
		return nil, err
	}

	if err := c.q.Enqueue(ctx, task); err != nil {
		return nil, err
	}

	return task, nil
}

// GetTask retrieves task state and details by task ID.
func (c *Client) GetTask(ctx context.Context, taskID string) (*types.Task, error) {
	return c.q.GetTask(ctx, taskID)
}

// GetStats returns current depth across all queues.
func (c *Client) GetStats(ctx context.Context) (*queue.QueueStats, error) {
	return c.q.GetStats(ctx)
}

// RetryDLQ re-queues an exhausted task from the dead-letter queue.
func (c *Client) RetryDLQ(ctx context.Context, taskID string) error {
	return c.q.RetryDLQTask(ctx, taskID)
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.q.Close()
}
