package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/pulsequeue/pulse-queue/pkg/client"
	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	namespace := flag.String("namespace", "default", "Queue namespace")
	flag.Parse()

	log.Println("[Producer] Connecting to PulseQueue cluster...")
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	q := queue.NewRedisQueue(rdb, *namespace)
	c := client.NewClient(q)
	ctx := context.Background()

	// 1. Critical Priority Task: Security Alert
	log.Println("-> Enqueueing CRITICAL priority task...")
	t1, err := c.Enqueue(ctx, "webhook:dispatch", map[string]string{
		"endpoint": "https://hooks.slack.com/services/sec-alerts",
		"event":    "SUSPICIOUS_LOGIN_ATTEMPT",
	}, types.WithPriority(types.PriorityCritical))
	if err != nil {
		log.Fatalf("Failed to enqueue t1: %v", err)
	}
	fmt.Printf("   Enqueued %s [Priority: %s]\n", t1.ID[:8], t1.Priority)

	// 2. High Priority Task: User Welcome Email
	log.Println("-> Enqueueing HIGH priority task...")
	t2, _ := c.Enqueue(ctx, "email:send", map[string]string{
		"to":       "alex.dev@example.com",
		"template": "welcome_onboarding",
	}, types.WithPriority(types.PriorityHigh))
	fmt.Printf("   Enqueued %s [Priority: %s]\n", t2.ID[:8], t2.Priority)

	// 3. Delayed Task: Follow-up Reminder (Scheduled 5s in the future)
	log.Println("-> Enqueueing DELAYED task (runs in 5 seconds)...")
	t3, _ := c.Enqueue(ctx, "email:send", map[string]string{
		"to":       "alex.dev@example.com",
		"template": "tutorial_day_1",
	}, types.WithDelay(5*time.Second))
	fmt.Printf("   Enqueued %s [Scheduled at: %s]\n", t3.ID[:8], t3.ScheduledAt.Format("15:04:05"))

	// 4. Idempotent / Deduplicated Task (Unique Key)
	log.Println("-> Enqueueing Deduplicated Task with UniqueKey='invoice-9921'...")
	t4, err := c.Enqueue(ctx, "payment:process", map[string]any{
		"amount":   499.00,
		"customer": "Stark Industries",
	}, types.WithUniqueKey("invoice-9921", 1*time.Minute))
	if err == nil {
		fmt.Printf("   Enqueued %s with unique key 'invoice-9921'\n", t4.ID[:8])
	}

	// Try enqueueing again with the exact same unique key - should be safely rejected!
	_, errDup := c.Enqueue(ctx, "payment:process", map[string]any{
		"amount":   499.00,
		"customer": "Stark Industries",
	}, types.WithUniqueKey("invoice-9921", 1*time.Minute))
	if errDup != nil {
		fmt.Printf("   Successfully prevented duplicate task: %v\n", errDup)
	}

	// 5. Flaky Task (Destined for DLQ after 3 retries)
	log.Println("-> Enqueueing Intentionally Failing Task (demos retry & DLQ)...")
	t5, _ := c.Enqueue(ctx, "payment:process", map[string]any{
		"amount":   1299.99,
		"customer": "Cyberdyne Systems",
		"fail":     true,
	}, types.WithMaxRetries(3))
	fmt.Printf("   Enqueued %s [MaxRetries: %d]\n", t5.ID[:8], t5.MaxRetries)

	// 6. Batch of background tasks
	log.Println("-> Enqueueing batch of 20 image thumbnails...")
	for i := 1; i <= 20; i++ {
		_, _ = c.Enqueue(ctx, "image:thumbnail", map[string]any{
			"url":    fmt.Sprintf("https://cdn.example.com/assets/photo_%d.jpg", i),
			"width":  800,
			"height": 600,
		}, types.WithPriority(types.PriorityDefault))
	}
	fmt.Println("   Batch of 20 images successfully enqueued!")

	stats, _ := c.GetStats(ctx)
	fmt.Printf("\nQueue Status Summary:\n- Active Queues: Critical=%d, High=%d, Default=%d\n- Delayed: %d\n- In-Flight: %d\n- DLQ: %d\n",
		stats.Critical, stats.High, stats.Default, stats.Delayed, stats.Processing, stats.DLQ)
}
