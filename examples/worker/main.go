package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsequeue/pulse-queue/pkg/engine"
	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/pkg/types"
	"github.com/redis/go-redis/v9"
)

func main() {
	concurrency := flag.Int("concurrency", 5, "Number of concurrent worker goroutines")
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	namespace := flag.String("namespace", "default", "Queue namespace")
	flag.Parse()

	log.Printf("[Worker] Initializing PulseQueue worker fleet (concurrency=%d)...", *concurrency)
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	q := queue.NewRedisQueue(rdb, *namespace)

	cfg := engine.DefaultServerConfig()
	cfg.Concurrency = *concurrency

	srv := engine.NewServer(q, cfg)

	// Register Task Handler: Transactional Email
	srv.RegisterFunc("email:send", func(ctx context.Context, task *types.Task) error {
		var p struct {
			To       string `json:"to"`
			Template string `json:"template"`
		}
		if err := task.UnmarshalPayload(&p); err != nil {
			return err
		}
		log.Printf("[Worker] Sending email template '%s' to %s (task=%s)", p.Template, p.To, task.ID[:8])
		time.Sleep(150 * time.Millisecond) // Simulate network latency
		return nil
	})

	// Register Task Handler: Image Processing
	srv.Register("image:thumbnail", func(ctx context.Context, task *types.Task) ([]byte, error) {
		var p struct {
			URL    string `json:"url"`
			Width  int    `json:"width"`
			Height int    `json:"height"`
		}
		if err := task.UnmarshalPayload(&p); err != nil {
			return nil, err
		}
		log.Printf("[Worker] Resizing image %s to %dx%d (task=%s)", p.URL, p.Width, p.Height, task.ID[:8])
		time.Sleep(300 * time.Millisecond) // Simulate CPU image transform
		result, _ := json.Marshal(map[string]any{
			"status": "resized",
			"path":   "/storage/thumbs/" + task.ID[:8] + ".webp",
		})
		return result, nil
	})

	// Register Task Handler: Webhook Dispatcher
	srv.RegisterFunc("webhook:dispatch", func(ctx context.Context, task *types.Task) error {
		var p struct {
			Endpoint string `json:"endpoint"`
			Event    string `json:"event"`
		}
		if err := task.UnmarshalPayload(&p); err != nil {
			return err
		}
		log.Printf("[Worker] Dispatched webhook event '%s' to %s (task=%s)", p.Event, p.Endpoint, task.ID[:8])
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	// Register Task Handler: Flaky Payment (Fails randomly to demo retry and DLQ)
	srv.RegisterFunc("payment:process", func(ctx context.Context, task *types.Task) error {
		var p struct {
			Amount   float64 `json:"amount"`
			Customer string  `json:"customer"`
			Fail     bool    `json:"fail"`
		}
		_ = task.UnmarshalPayload(&p)

		if p.Fail || rand.Float32() < 0.4 {
			log.Printf("[Worker] Payment gateway error for customer %s (attempt %d/%d)", p.Customer, task.RetryCount+1, task.MaxRetries)
			return errors.New("upstream payment gateway 503 Service Unavailable")
		}

		log.Printf("[Worker] Payment of $%.2f successfully settled for %s", p.Amount, p.Customer)
		return nil
	})

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("[Worker] Shutdown signal received. Initiating graceful drain...")
		srv.Stop()
		cancel()
	}()

	if err := srv.Start(ctx); err != nil {
		fmt.Printf("Worker stopped: %v\n", err)
	}
}
