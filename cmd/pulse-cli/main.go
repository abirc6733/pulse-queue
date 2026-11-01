package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/abirc6733/pulse-queue/pkg/client"
	"github.com/abirc6733/pulse-queue/pkg/queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	namespace := flag.String("namespace", "default", "Queue namespace")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPassword,
	})
	q := queue.NewRedisQueue(rdb, *namespace)
	c := client.NewClient(q)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	command := args[0]
	switch command {
	case "stats":
		stats, err := c.GetStats(ctx)
		if err != nil {
			log.Fatalf("Error fetching stats: %v", err)
		}
		fmt.Printf("\n--- PulseQueue Stats [%s] ---\n", *namespace)
		fmt.Printf("Critical:   %d\n", stats.Critical)
		fmt.Printf("High:       %d\n", stats.High)
		fmt.Printf("Default:    %d\n", stats.Default)
		fmt.Printf("Low:        %d\n", stats.Low)
		fmt.Printf("Delayed:    %d\n", stats.Delayed)
		fmt.Printf("Processing: %d\n", stats.Processing)
		fmt.Printf("DLQ:        %d\n", stats.DLQ)
		fmt.Printf("Completed:  %d\n", stats.Completed)
		fmt.Printf("Workers:    %d\n\n", stats.Workers)

	case "workers":
		workers, err := q.GetWorkers(ctx)
		if err != nil {
			log.Fatalf("Error fetching workers: %v", err)
		}
		fmt.Printf("\n--- Active Worker Nodes (%d) ---\n", len(workers))
		for _, w := range workers {
			fmt.Printf("ID: %s | Host: %s | Concurrency: %d | Active: %d | Last Seen: %s\n",
				w.WorkerID, w.Hostname, w.Concurrency, w.ActiveJobs, w.LastSeen.Format("15:04:05"))
		}
		fmt.Println()

	case "dlq":
		tasks, total, err := q.GetDLQ(ctx, 0, 50)
		if err != nil {
			log.Fatalf("Error fetching DLQ: %v", err)
		}
		fmt.Printf("\n--- Dead Letter Queue (Total: %d) ---\n", total)
		for _, t := range tasks {
			fmt.Printf("Task ID: %s | Type: %s | Retries: %d/%d | Error: %s\n",
				t.ID, t.Type, t.RetryCount, t.MaxRetries, t.LastError)
		}
		fmt.Println()

	case "retry":
		if len(args) < 2 {
			log.Fatalf("Usage: pulse-cli retry <task-id>")
		}
		taskID := args[1]
		if err := c.RetryDLQ(ctx, taskID); err != nil {
			log.Fatalf("Error re-queueing DLQ task: %v", err)
		}
		fmt.Printf("Task %s successfully re-queued from DLQ.\n", taskID)

	case "enqueue":
		if len(args) < 2 {
			log.Fatalf("Usage: pulse-cli enqueue <type> [json-payload]")
		}
		taskType := args[1]
		var payload any = map[string]string{"message": "hello from CLI"}
		if len(args) >= 3 {
			var parsed any
			if err := json.Unmarshal([]byte(args[2]), &parsed); err == nil {
				payload = parsed
			}
		}
		task, err := c.Enqueue(ctx, taskType, payload)
		if err != nil {
			log.Fatalf("Error enqueueing: %v", err)
		}
		fmt.Printf("Enqueued task %s (type=%s, priority=%s)\n", task.ID, task.Type, task.Priority)

	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`PulseQueue CLI - Background Task Manager
Usage:
  pulse-cli [options] <command> [args]

Commands:
  stats                      Display current queue depth and metrics
  workers                    List active worker nodes
  dlq                        Inspect dead-letter queue items
  retry <task-id>            Re-queue a failed task from the DLQ
  enqueue <type> [payload]   Enqueue a new task

Options:
  --redis <addr>             Redis server address (default: localhost:6379)
  --namespace <name>         Queue namespace (default: default)`)
}
