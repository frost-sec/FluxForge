package postgres

import (
	"context"
	"testing"
)

func TestActiveSLOMetadataAndLogicalDeadline(t *testing.T) {
	s := testStore(t)
	w := publishFixture(t, s, true)
	r := submitFixture(t, s, w)
	views, e := s.ActiveSLOViews(context.Background())
	if e != nil || len(views) != 1 || views[0].Run.ID != r.ID || len(views[0].Nodes) != 3 {
		t.Fatalf("SLO metadata: %+v %v", views, e)
	}
	ready, e := s.ListReady(context.Background())
	if e != nil || len(ready) != 2 || ready[0].SLODeadline.IsZero() {
		t.Fatalf("function SLO missing: %+v %v", ready, e)
	}
}
