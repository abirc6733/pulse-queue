.PHONY: all build test bench run-server run-worker run-producer clean docker-up docker-down

all: build test

build:
	@mkdir -p bin
	go build -o bin/pulse-server ./cmd/pulse-server
	go build -o bin/pulse-cli ./cmd/pulse-cli
	go build -o bin/pulse-worker-example ./examples/worker
	go build -o bin/pulse-producer-example ./examples/producer

test:
	go test -v -race ./pkg/...

bench:
	go test -bench=. -benchmem ./benchmark/...

run-server:
	go run ./cmd/pulse-server

run-worker:
	go run ./examples/worker -concurrency=10

run-producer:
	go run ./examples/producer

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

clean:
	rm -rf bin/
