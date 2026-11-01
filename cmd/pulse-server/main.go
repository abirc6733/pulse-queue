package main

import (
	"flag"
	"log"

	"github.com/pulsequeue/pulse-queue/pkg/queue"
	"github.com/pulsequeue/pulse-queue/web"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	namespace := flag.String("namespace", "default", "Queue namespace")
	port := flag.Int("port", 8080, "Dashboard HTTP port")
	flag.Parse()

	log.Printf("[PulseServer] Connecting to Redis at %s (namespace=%s)...", *redisAddr, *namespace)
	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPassword,
	})

	q := queue.NewRedisQueue(rdb, *namespace)
	srv := web.NewDashboardServer(q, *port)

	log.Printf("[PulseServer] Dashboard and Management API running at http://localhost:%d", *port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server exited: %v", err)
	}
}
