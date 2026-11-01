package client

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

func TestClientLifecycle(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available on localhost:6379")
	}

	ns := "test-client-" + uuid.New().String()[:8]
	q := queue.NewRedisQueue(rdb, ns)
	c := NewClient(q)
	defer c.Close()

	// Enqueue via client SDK
	task, err := c.Enqueue(context.Background(), "user:signup", map[string]string{
		"email": "test@example.com",
	}, types.WithPriority(types.PriorityHigh))
	if err != nil {
		t.Fatalf("client enqueue failed: %v", err)
	}

	// Fetch task via client SDK
	fetched, err := c.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("client get task failed: %v", err)
	}

	if fetched.Type != "user:signup" {
		t.Errorf("expected type 'user:signup', got %s", fetched.Type)
	}

	// Fetch stats via client SDK
	stats, err := c.GetStats(context.Background())
	if err != nil {
		t.Fatalf("client get stats failed: %v", err)
	}

	if stats.High != 1 {
		t.Errorf("expected 1 high priority task, got %d", stats.High)
	}
}
