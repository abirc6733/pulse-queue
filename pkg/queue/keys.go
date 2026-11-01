package queue

import (
	"fmt"
	"github.com/pulsequeue/pulse-queue/pkg/types"
)

// KeyManager handles consistent Redis key formatting under an isolated namespace.
type KeyManager struct {
	Namespace string
}

// NewKeyManager creates a new KeyManager for the given namespace (defaults to "default").
func NewKeyManager(namespace string) *KeyManager {
	if namespace == "" {
		namespace = "default"
	}
	return &KeyManager{Namespace: namespace}
}

func (k *KeyManager) prefix() string {
	return fmt.Sprintf("pulse:%s", k.Namespace)
}

// QueueKey returns the Redis list key for active tasks of a specific priority.
func (k *KeyManager) QueueKey(p types.Priority) string {
	return fmt.Sprintf("%s:queue:%s", k.prefix(), p)
}

// DelayedKey returns the sorted set key for scheduled/delayed tasks.
func (k *KeyManager) DelayedKey() string {
	return fmt.Sprintf("%s:delayed", k.prefix())
}

// ProcessingKey returns the sorted set key for in-flight tasks under visibility timeout.
func (k *KeyManager) ProcessingKey() string {
	return fmt.Sprintf("%s:processing", k.prefix())
}

// DLQKey returns the dead-letter queue key for permanently failed tasks.
func (k *KeyManager) DLQKey() string {
	return fmt.Sprintf("%s:dlq", k.prefix())
}

// CompletedKey returns the sorted set key for recently completed tasks.
func (k *KeyManager) CompletedKey() string {
	return fmt.Sprintf("%s:completed", k.prefix())
}

// TaskKey returns the key storing serialized metadata for a specific task.
func (k *KeyManager) TaskKey(taskID string) string {
	return fmt.Sprintf("%s:task:%s", k.prefix(), taskID)
}

// UniqueKey returns the key used to enforce idempotency deduplication.
func (k *KeyManager) UniqueKey(uniqueKey string) string {
	return fmt.Sprintf("%s:unique:%s", k.prefix(), uniqueKey)
}

// WorkersKey returns the hash key storing active worker heartbeats.
func (k *KeyManager) WorkersKey() string {
	return fmt.Sprintf("%s:workers", k.prefix())
}

// StatsKey returns the key for cumulative task metrics.
func (k *KeyManager) StatsKey(metric string) string {
	return fmt.Sprintf("%s:stats:%s", k.prefix(), metric)
}
