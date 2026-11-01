package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/abirc6733/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

var (
	ErrDuplicateTask = errors.New("task with unique key already exists")
	ErrTaskNotFound  = errors.New("task not found")
	ErrNilTask       = errors.New("task cannot be nil")
)

type RedisQueue struct {
	client *redis.Client
	km     *KeyManager
}

// NewRedisQueue initializes a Redis-backed Task Queue.
func NewRedisQueue(client *redis.Client, namespace string) *RedisQueue {
	return &RedisQueue{
		client: client,
		km:     NewKeyManager(namespace),
	}
}

// Enqueue inserts a task into Redis atomically with scheduling and deduplication.
func (rq *RedisQueue) Enqueue(ctx context.Context, task *types.Task) error {
	if task == nil {
		return ErrNilTask
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	uniqueKey := ""
	uniqueTTLSec := int64(0)
	if task.UniqueKey != "" {
		uniqueKey = rq.km.UniqueKey(task.UniqueKey)
		if task.UniqueTTL > 0 {
			uniqueTTLSec = int64(task.UniqueTTL.Seconds())
		} else {
			uniqueTTLSec = 300 // default 5 minutes
		}
	}

	nowMs := time.Now().UTC().UnixMilli()
	scheduledMs := task.ScheduledAt.UnixMilli()

	keys := []string{
		rq.km.TaskKey(task.ID),
		rq.km.QueueKey(task.Priority),
		rq.km.DelayedKey(),
		uniqueKey,
	}

	args := []any{
		task.ID,
		string(taskBytes),
		scheduledMs,
		nowMs,
		uniqueTTLSec,
	}

	res, err := luaEnqueue.Run(ctx, rq.client, keys, args...).Int()
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	if res == 0 {
		return ErrDuplicateTask
	}

	return nil
}

// Dequeue retrieves the next highest-priority task and sets an initial visibility timeout.
func (rq *RedisQueue) Dequeue(ctx context.Context, workerID string) (*types.Task, error) {
	keys := []string{
		rq.km.QueueKey(types.PriorityCritical),
		rq.km.QueueKey(types.PriorityHigh),
		rq.km.QueueKey(types.PriorityDefault),
		rq.km.QueueKey(types.PriorityLow),
		rq.km.ProcessingKey(),
	}

	// Default visibility deadline: 5 minutes if not overridden
	deadlineMs := time.Now().UTC().Add(5 * time.Minute).UnixMilli()

	res, err := luaDequeue.Run(ctx, rq.client, keys, workerID, deadlineMs).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue lua error: %w", err)
	}

	if res == nil {
		return nil, nil
	}

	taskID, ok := res.(string)
	if !ok || taskID == "" {
		return nil, nil
	}

	task, err := rq.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	// Set dynamic visibility timeout based on task.Timeout
	if task.Timeout > 0 {
		dynamicDeadlineMs := float64(time.Now().UTC().Add(task.Timeout).UnixMilli())
		_ = rq.client.ZAdd(ctx, rq.km.ProcessingKey(), redis.Z{
			Score:  dynamicDeadlineMs,
			Member: taskID,
		}).Err()
	}

	task.State = types.StateActive
	now := time.Now().UTC()
	task.StartedAt = &now
	task.WorkerID = workerID

	return task, nil
}

// Ack marks the task as completed successfully.
func (rq *RedisQueue) Ack(ctx context.Context, task *types.Task, result []byte) error {
	now := time.Now().UTC()
	task.State = types.StateCompleted
	task.CompletedAt = &now
	if len(result) > 0 {
		task.Result = result
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}

	keys := []string{
		rq.km.ProcessingKey(),
		rq.km.TaskKey(task.ID),
		rq.km.CompletedKey(),
	}

	args := []any{
		task.ID,
		string(taskBytes),
		now.UnixMilli(),
		86400, // 24-hour retention
	}

	_, err = luaAck.Run(ctx, rq.client, keys, args...).Result()
	return err
}

// Nack handles task failure with exponential backoff retry or routing to Dead Letter Queue (DLQ).
func (rq *RedisQueue) Nack(ctx context.Context, task *types.Task, taskErr error) error {
	task.RetryCount++
	if taskErr != nil {
		task.LastError = taskErr.Error()
	}

	isDLQ := "0"
	nextRunMs := int64(0)

	if task.RetryCount >= task.MaxRetries {
		isDLQ = "1"
		task.State = types.StateDead
	} else {
		task.State = types.StateRetry
		// Exponential backoff: base * 2^(retry-1) + jitter
		base := 2.0
		backoffSec := math.Pow(base, float64(task.RetryCount))
		jitter := rand.Float64() * 0.5 * backoffSec
		delay := time.Duration((backoffSec + jitter) * float64(time.Second))
		task.ScheduledAt = time.Now().UTC().Add(delay)
		nextRunMs = task.ScheduledAt.UnixMilli()
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}

	keys := []string{
		rq.km.ProcessingKey(),
		rq.km.TaskKey(task.ID),
		rq.km.DelayedKey(),
		rq.km.DLQKey(),
	}

	args := []any{
		task.ID,
		string(taskBytes),
		isDLQ,
		nextRunMs,
	}

	_, err = luaNack.Run(ctx, rq.client, keys, args...).Result()
	return err
}

// GetTask loads task metadata from Redis.
func (rq *RedisQueue) GetTask(ctx context.Context, taskID string) (*types.Task, error) {
	data, err := rq.client.Get(ctx, rq.km.TaskKey(taskID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	var task types.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// MigrateDelayed moves tasks ready to be executed from the delayed set to active queues.
func (rq *RedisQueue) MigrateDelayed(ctx context.Context, batchSize int) (int, error) {
	nowMs := time.Now().UTC().UnixMilli()
	keys := []string{rq.km.DelayedKey()}
	args := []any{nowMs, batchSize}

	result, err := luaMigrateDelayed.Run(ctx, rq.client, keys, args...).Slice()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range result {
		taskID, ok := item.(string)
		if !ok {
			continue
		}
		task, err := rq.GetTask(ctx, taskID)
		if err != nil {
			continue
		}

		task.State = types.StatePending
		taskBytes, _ := json.Marshal(task)

		pipe := rq.client.Pipeline()
		pipe.Set(ctx, rq.km.TaskKey(task.ID), string(taskBytes), 0)
		pipe.LPush(ctx, rq.km.QueueKey(task.Priority), task.ID)
		_, _ = pipe.Exec(ctx)
		count++
	}

	return count, nil
}

// RequeueOrphans reclaims orphaned tasks where a worker crashed without acking before deadline.
func (rq *RedisQueue) RequeueOrphans(ctx context.Context, batchSize int) (int, error) {
	nowMs := time.Now().UTC().UnixMilli()
	keys := []string{rq.km.ProcessingKey()}
	args := []any{nowMs, batchSize}

	result, err := luaRequeueOrphans.Run(ctx, rq.client, keys, args...).Slice()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range result {
		taskID, ok := item.(string)
		if !ok {
			continue
		}
		task, err := rq.GetTask(ctx, taskID)
		if err != nil {
			continue
		}

		// Fail and retry orphan task
		_ = rq.Nack(ctx, task, fmt.Errorf("worker visibility timeout expired (node crash recovery)"))
		count++
	}

	return count, nil
}

// RegisterHeartbeat records worker health and active state.
func (rq *RedisQueue) RegisterHeartbeat(ctx context.Context, workerID string, hb WorkerHeartbeat) error {
	data, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	return rq.client.HSet(ctx, rq.km.WorkersKey(), workerID, string(data)).Err()
}

// GetWorkers retrieves all reporting worker nodes and prunes dead ones (no heartbeat > 30s).
func (rq *RedisQueue) GetWorkers(ctx context.Context) ([]WorkerHeartbeat, error) {
	all, err := rq.client.HGetAll(ctx, rq.km.WorkersKey()).Result()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	var active []WorkerHeartbeat
	for id, val := range all {
		var hb WorkerHeartbeat
		if err := json.Unmarshal([]byte(val), &hb); err == nil {
			if now.Sub(hb.LastSeen) > 30*time.Second {
				// Worker is dead, remove
				_ = rq.client.HDel(ctx, rq.km.WorkersKey(), id).Err()
			} else {
				active = append(active, hb)
			}
		}
	}
	return active, nil
}

// GetStats returns current depth across all queues.
func (rq *RedisQueue) GetStats(ctx context.Context) (*QueueStats, error) {
	pipe := rq.client.Pipeline()
	critCmd := pipe.LLen(ctx, rq.km.QueueKey(types.PriorityCritical))
	highCmd := pipe.LLen(ctx, rq.km.QueueKey(types.PriorityHigh))
	defCmd := pipe.LLen(ctx, rq.km.QueueKey(types.PriorityDefault))
	lowCmd := pipe.LLen(ctx, rq.km.QueueKey(types.PriorityLow))
	delCmd := pipe.ZCard(ctx, rq.km.DelayedKey())
	procCmd := pipe.ZCard(ctx, rq.km.ProcessingKey())
	dlqCmd := pipe.LLen(ctx, rq.km.DLQKey())
	compCmd := pipe.ZCard(ctx, rq.km.CompletedKey())
	workCmd := pipe.HLen(ctx, rq.km.WorkersKey())

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	return &QueueStats{
		Critical:   critCmd.Val(),
		High:       highCmd.Val(),
		Default:    defCmd.Val(),
		Low:        lowCmd.Val(),
		Delayed:    delCmd.Val(),
		Processing: procCmd.Val(),
		DLQ:        dlqCmd.Val(),
		Completed:  compCmd.Val(),
		Workers:    int(workCmd.Val()),
	}, nil
}

// GetDLQ retrieves dead-lettered tasks for manual inspection.
func (rq *RedisQueue) GetDLQ(ctx context.Context, start, stop int64) ([]*types.Task, int64, error) {
	total, err := rq.client.LLen(ctx, rq.km.DLQKey()).Result()
	if err != nil {
		return nil, 0, err
	}

	ids, err := rq.client.LRange(ctx, rq.km.DLQKey(), start, stop).Result()
	if err != nil {
		return nil, 0, err
	}

	var tasks []*types.Task
	for _, id := range ids {
		if t, err := rq.GetTask(ctx, id); err == nil {
			tasks = append(tasks, t)
		}
	}

	return tasks, total, nil
}

// RetryDLQTask re-queues a failed task from the DLQ for re-execution.
func (rq *RedisQueue) RetryDLQTask(ctx context.Context, taskID string) error {
	task, err := rq.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	task.State = types.StatePending
	task.RetryCount = 0
	task.LastError = ""

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}

	pipe := rq.client.Pipeline()
	pipe.LRem(ctx, rq.km.DLQKey(), 1, taskID)
	pipe.Set(ctx, rq.km.TaskKey(taskID), string(taskBytes), 0)
	pipe.LPush(ctx, rq.km.QueueKey(task.Priority), taskID)
	_, err = pipe.Exec(ctx)
	return err
}

func (rq *RedisQueue) Close() error {
	return rq.client.Close()
}
