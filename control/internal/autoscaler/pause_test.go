package autoscaler

import (
	"context"
	"testing"
	"time"
)

func TestPausedPoolNeverCreatesReplacement(t *testing.T) {
	p := &provider{s: PoolSnapshot{ReadyTasks: 10, ReadyWork: time.Second}}
	d := &driver{}
	c := New(Config{Paused: true}, p, &policy{}, d)
	r, e := c.Reconcile(context.Background())
	if e != nil || len(r.Created) != 0 {
		t.Fatalf("paused fleet created capacity: %+v %v", r, e)
	}
}
