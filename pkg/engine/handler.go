package engine

import (
	"context"
	"fmt"

	"github.com/pulsequeue/pulse-queue/pkg/types"
)

// Handler processes a task and optionally returns a result or error.
type Handler func(ctx context.Context, task *types.Task) ([]byte, error)

// HandlerFunc is an adapter allowing standard functions to be used as Handlers.
type HandlerFunc func(ctx context.Context, task *types.Task) error

// HandlerRegistry stores mapping from task types to execution handlers.
type HandlerRegistry struct {
	handlers map[string]Handler
}

// NewHandlerRegistry initializes an empty handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]Handler),
	}
}

// Register registers a handler for a task type.
func (hr *HandlerRegistry) Register(taskType string, h Handler) {
	hr.handlers[taskType] = h
}

// RegisterFunc registers a handler without result return.
func (hr *HandlerRegistry) RegisterFunc(taskType string, fn HandlerFunc) {
	hr.handlers[taskType] = func(ctx context.Context, task *types.Task) ([]byte, error) {
		return nil, fn(ctx, task)
	}
}

// Get finds a handler for the given task type.
func (hr *HandlerRegistry) Get(taskType string) (Handler, error) {
	h, ok := hr.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("no handler registered for task type: %s", taskType)
	}
	return h, nil
}
