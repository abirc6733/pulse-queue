package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/abirc6733/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

func getTestClient(t *testing.T) (*redis.Client, func()) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping Redis integration test: Redis not reachable on localhost:6379 (%v)", err)
	}

	ns := "test-" + uuid.New().String()[:8]
	km := NewKeyManager(ns)

	cleanup := func() {
		// Clean up keys created during the test
		ctx := context.Background()
		rdb.Del(ctx,
			km.QueueKey(types.PriorityCritical),
			km.QueueKey(types.PriorityHigh),
			km.QueueKey(types.PriorityDefault),
			km.QueueKey(types.PriorityLow),
			km.DelayedKey(),
			km.ProcessingKey(),
			km.DLQKey(),
			km.CompletedKey(),
			km.WorkersKey(),
		)
		rdb.Close()
	}

	return rdb, cleanup
}

func TestEnqueueAndPriorityDequeue(t *testing.T) {
	rdb, cleanup := getTestClient(t)
	defer cleanup()

	ctx := context.Background()
	ns := "test-" + uuid.New().String()[:8]
	q := NewRedisQueue(rdb, ns)

	// Enqueue Default priority first
	defaultTask, _ := types.NewTask("task:default", map[string]string{"foo": "default"}, types.WithPriority(types.PriorityDefault))
	if err := q.Enqueue(ctx, defaultTask); err != nil {
		t.Fatalf("failed to enqueue default task: %v", err)
	}

	// Enqueue Critical priority second
	critTask, _ := types.NewTask("task:critical", map[string]string{"foo": "critical"}, types.WithPriority(types.PriorityCritical))
	if err := q.Enqueue(ctx, critTask); err != nil {
		t.Fatalf("failed to enqueue critical task: %v", err)
	}

	// First dequeue must return the CRITICAL task, even though default was enqueued first!
	first, err := q.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if first == nil || first.ID != critTask.ID {
		t.Fatalf("expected critical task %s, got %+v", critTask.ID, first)
	}

	// Second dequeue returns default task
	second, err := q.Dequeue(ctx, "worker-1")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if second == nil || second.ID != defaultTask.ID {
		t.Fatalf("expected default task %s, got %+v", defaultTask.ID, second)
	}
}

func TestIdempotencyUniqueKey(t *testing.T) {
	rdb, cleanup := getTestClient(t)
	defer cleanup()

	ctx := context.Background()
	ns := "test-" + uuid.New().String()[:8]
	q := NewRedisQueue(rdb, ns)

	task1, _ := types.NewTask("billing:charge", map[string]string{"invoice": "inv-101"},
		types.WithUniqueKey("inv-101", 10*time.Second))

	if err := q.Enqueue(ctx, task1); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	// Enqueue same unique key - should return ErrDuplicateTask
	task2, _ := types.NewTask("billing:charge", map[string]string{"invoice": "inv-101"},
		types.WithUniqueKey("inv-101", 10*time.Second))

	err := q.Enqueue(ctx, task2)
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("expected ErrDuplicateTask, got %v", err)
	}
}

func TestAckWorkflow(t *testing.T) {
	rdb, cleanup := getTestClient(t)
	defer cleanup()

	ctx := context.Background()
	ns := "test-" + uuid.New().String()[:8]
	q := NewRedisQueue(rdb, ns)

	task, _ := types.NewTask("calc:add", map[string]int{"a": 2, "b": 3})
	_ = q.Enqueue(ctx, task)

	popped, _ := q.Dequeue(ctx, "worker-1")
	if popped == nil {
		t.Fatalf("expected task, got nil")
	}

	result := []byte(`{"sum": 5}`)
	if err := q.Ack(ctx, popped, result); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	completed, err := q.GetTask(ctx, popped.ID)
	if err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	if completed.State != types.StateCompleted {
		t.Errorf("expected state completed, got %s", completed.State)
	}
}

func TestNackAndDLQRouting(t *testing.T) {
	rdb, cleanup := getTestClient(t)
	defer cleanup()

	ctx := context.Background()
	ns := "test-" + uuid.New().String()[:8]
	q := NewRedisQueue(rdb, ns)

	task, _ := types.NewTask("failing:job", "payload", types.WithMaxRetries(2))
	_ = q.Enqueue(ctx, task)

	// Attempt 1
	p1, _ := q.Dequeue(ctx, "worker-1")
	_ = q.Nack(ctx, p1, errors.New("err 1"))
	t1, _ := q.GetTask(ctx, p1.ID)
	if t1.State != types.StateRetry || t1.RetryCount != 1 {
		t.Errorf("expected retry state after 1st failure, got state=%s retry=%d", t1.State, t1.RetryCount)
	}

	// Migrate delayed back
	time.Sleep(100 * time.Millisecond)
	p1.ScheduledAt = time.Now().UTC().Add(-1 * time.Second) // Force ready
	_ = q.Enqueue(ctx, p1)

	// Attempt 2 (exhausts max retries = 2)
	p2, _ := q.Dequeue(ctx, "worker-1")
	_ = q.Nack(ctx, p2, errors.New("err 2"))
	t2, _ := q.GetTask(ctx, p2.ID)
	if t2.State != types.StateDead || t2.RetryCount != 2 {
		t.Errorf("expected dead state after 2nd failure, got state=%s retry=%d", t2.State, t2.RetryCount)
	}

	// Verify task is in DLQ
	dlqTasks, total, err := q.GetDLQ(ctx, 0, 10)
	if err != nil {
		t.Fatalf("failed to get DLQ: %v", err)
	}
	if total < 1 || len(dlqTasks) < 1 {
		t.Fatalf("expected task in DLQ, found total=%d", total)
	}
}
