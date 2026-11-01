package benchmark

import (
	"context"
	"fmt"
	"testing"

	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

func BenchmarkEnqueue(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		b.Skip("Redis not available on localhost:6379")
	}

	q := queue.NewRedisQueue(rdb, "bench")
	payload := map[string]string{"bench": "data"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			task, _ := types.NewTask(fmt.Sprintf("bench:task:%d", i), payload)
			_ = q.Enqueue(ctx, task)
		}
	})
}
