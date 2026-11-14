package scheduler

import (
	"errors"
	"testing"
)

func TestOldSessionExpiryDoesNotDeactivateNewBoot(t *testing.T) {
	s := New(Config{})
	s.UpsertRunner(Runner{ID: "runner", SessionID: "new", Active: true, AvailableSlots: 1})
	s.RemoveSession("runner", "old")
	if _, e := s.Reserve("runner", 1); e != nil {
		t.Fatalf("old boot fenced new boot: %v", e)
	}
	s.RemoveSession("runner", "new")
	if _, e := s.Reserve("runner", 1); !errors.Is(e, ErrRunnerUnavailable) {
		t.Fatalf("expired session remained active: %v", e)
	}
}
