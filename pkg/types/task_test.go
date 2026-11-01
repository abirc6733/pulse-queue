package types

import (
	"testing"
	"time"
)

func TestNewTaskDefaults(t *testing.T) {
	task, err := NewTask("email:send", map[string]string{"to": "user@example.com"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task.ID == "" {
		t.Errorf("expected non-empty UUID, got empty")
	}
	if task.Type != "email:send" {
		t.Errorf("expected type 'email:send', got %s", task.Type)
	}
	if task.Priority != PriorityDefault {
		t.Errorf("expected priority default, got %s", task.Priority)
	}
	if task.MaxRetries != 3 {
		t.Errorf("expected 3 max retries, got %d", task.MaxRetries)
	}
	if task.State != StatePending {
		t.Errorf("expected pending state, got %s", task.State)
	}
}

func TestTaskOptions(t *testing.T) {
	future := time.Now().UTC().Add(10 * time.Minute)
	task, err := NewTask("payment:process", map[string]float64{"amt": 100},
		WithPriority(PriorityCritical),
		WithMaxRetries(5),
		WithTimeout(15*time.Second),
		WithSchedule(future),
		WithUniqueKey("pay-123", 5*time.Minute),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if task.Priority != PriorityCritical {
		t.Errorf("expected critical priority, got %s", task.Priority)
	}
	if task.MaxRetries != 5 {
		t.Errorf("expected 5 max retries, got %d", task.MaxRetries)
	}
	if task.Timeout != 15*time.Second {
		t.Errorf("expected 15s timeout, got %v", task.Timeout)
	}
	if task.State != StateScheduled {
		t.Errorf("expected scheduled state, got %s", task.State)
	}
	if task.UniqueKey != "pay-123" {
		t.Errorf("expected unique key 'pay-123', got %s", task.UniqueKey)
	}
}

func TestTaskPayloadUnmarshal(t *testing.T) {
	type Custom struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	task, _ := NewTask("test", Custom{Name: "Alice", Age: 30})

	var c Custom
	if err := task.UnmarshalPayload(&c); err != nil {
		t.Fatalf("failed unmarshal: %v", err)
	}

	if c.Name != "Alice" || c.Age != 30 {
		t.Errorf("unexpected unmarshaled values: %+v", c)
	}
}
