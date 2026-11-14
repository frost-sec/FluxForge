package postgres

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"

	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/projection"
	"fluxforge-control/internal/scheduler"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Every test owns a fresh schema; the existing database and other schemas are
// preserved. This suite is deliberately skipped without an explicit test DSN.
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("FF_TEST_DSN")
	if dsn == "" {
		t.Skip("set FF_TEST_DSN to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "ff_test_" + strings.ReplaceAll(domain.NewID(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+quoted); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 12
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, e := admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	if e = Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	return New(pool)
}
func publishFixture(t *testing.T, s *Store, fanIn bool) domain.WorkflowSpec {
	t.Helper()
	ctx := context.Background()
	f := domain.FunctionSpec{ID: "echo", Version: "v1", Artifact: domain.ObjectRef{ID: domain.NewID(), SHA256: strings.Repeat("a", 64), Size: 100}, TimeoutMS: 10000}
	if e := domain.NormalizeFunction(&f); e != nil {
		t.Fatal(e)
	}
	if e := s.PublishFunction(ctx, f); e != nil {
		t.Fatal(e)
	}
	step := func(name string, deps ...string) domain.StepSpec {
		return domain.StepSpec{Name: name, Function: domain.FunctionRef{ID: f.ID, Version: f.Version}, DependsOn: deps, Retry: domain.RetryPolicy{MaxAttempts: 3, InitialBackoffMS: 1, MaxBackoffMS: 5}}
	}
	w := domain.WorkflowSpec{ID: "test", Version: "v1", Steps: []domain.StepSpec{step("A")}, OutputStep: "A"}
	if fanIn {
		w.Steps = []domain.StepSpec{step("B"), step("C"), step("D", "B", "C")}
		w.OutputStep = "D"
	}
	if e := domain.NormalizeWorkflow(&w); e != nil {
		t.Fatal(e)
	}
	if e := s.PublishWorkflow(ctx, w); e != nil {
		t.Fatal(e)
	}
	return w
}
func submitFixture(t *testing.T, s *Store, w domain.WorkflowSpec) domain.Run {
	t.Helper()
	r, _, e := s.Submit(context.Background(), domain.SubmitCommand{WorkflowID: w.ID, Version: w.Version, IdempotencyKey: domain.NewID(), Input: domain.PayloadRef{Inline: json.RawMessage(`{}`)}, Priority: 50, SLOMS: 5000})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func registerFixture(t *testing.T, s *Store, slots int32) domain.RunnerSession {
	t.Helper()
	r, e := s.Register(context.Background(), domain.RunnerSession{RunnerID: domain.NewID(), BootID: domain.NewID(), Slots: slots, MemoryBytes: 1 << 30, CPUMillis: 4000})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func claimFixture(t *testing.T, s *Store, r domain.RunnerSession, n int32) domain.ClaimResult {
	t.Helper()
	ready, e := s.ListReady(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.Claim(context.Background(), domain.ClaimCommand{SessionID: r.ID, RequestID: domain.NewID(), RequestedSlots: n, Candidates: ready})
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func successful(a domain.Assignment) domain.Completion {
	return domain.Completion{SessionID: a.Attempt.SessionID, AttemptID: a.Attempt.ID, LeaseToken: a.Attempt.LeaseToken, Outcome: "SUCCEEDED", DurationMS: 1, Output: &domain.PayloadRef{Inline: json.RawMessage(`{"value":1}`)}}
}
func TestSubmitReplayAndConflictingKey(t *testing.T) {
	s := testStore(t)
	w := publishFixture(t, s, false)
	ctx := context.Background()
	c := domain.SubmitCommand{WorkflowID: w.ID, Version: w.Version, IdempotencyKey: "same-request", Input: domain.PayloadRef{Inline: json.RawMessage(`{}`)}, Priority: 50, SLOMS: 1000}
	first, _, e := s.Submit(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	c.TraceParent = "different-trace-context"
	second, dup, e := s.Submit(ctx, c)
	if e != nil || !dup || first.ID != second.ID {
		t.Fatalf("submit replay: %+v %v %v", second, dup, e)
	}
	c.Priority++
	if _, _, e = s.Submit(ctx, c); e == nil {
		t.Fatal("conflicting key accepted")
	}
}
func TestConcurrentFanInAndDuplicateCompletion(t *testing.T) {
	s := testStore(t)
	w := publishFixture(t, s, true)
	r := submitFixture(t, s, w)
	runner := registerFixture(t, s, 2)
	batch := claimFixture(t, s, runner, 2)
	if len(batch.Assignments) != 2 {
		t.Fatalf("claimed %d roots", len(batch.Assignments))
	}
	ctx := context.Background()
	for _, a := range batch.Assignments {
		if _, e := s.Started(ctx, runner.ID, a.Attempt.ID, a.Attempt.LeaseToken); e != nil {
			t.Fatal(e)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, a := range batch.Assignments {
		wg.Add(1)
		go func(a domain.Assignment) { defer wg.Done(); _, e := s.Complete(ctx, successful(a)); errs <- e }(a)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	for _, a := range batch.Assignments {
		v, e := s.Complete(ctx, successful(a))
		if e != nil || !v.Duplicate {
			t.Fatalf("completion replay %v %+v", e, v)
		}
	}
	var deps, events int
	var state string
	if e := s.Pool.QueryRow(ctx, `SELECT remaining_deps,state FROM invocations WHERE run_id=$1 AND step_name='D'`, r.ID).Scan(&deps, &state); e != nil {
		t.Fatal(e)
	}
	if deps != 0 || state != "READY" {
		t.Fatalf("fan-in deps=%d state=%s", deps, state)
	}
	if e := s.Pool.QueryRow(ctx, `SELECT count(*) FROM scheduler_outbox o JOIN invocations i ON i.id=o.invocation_id WHERE i.run_id=$1 AND i.step_name='D' AND o.kind='READY'`, r.ID).Scan(&events); e != nil {
		t.Fatal(e)
	}
	if events != 1 {
		t.Fatalf("successor queued %d times", events)
	}
	final := claimFixture(t, s, runner, 1)
	if len(final.Assignments) != 1 {
		t.Fatal("join not claimable")
	}
	a := final.Assignments[0]
	if _, e := s.Started(ctx, runner.ID, a.Attempt.ID, a.Attempt.LeaseToken); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Complete(ctx, successful(a)); e != nil {
		t.Fatal(e)
	}
	ended, e := s.Finalize(ctx, r.ID)
	if e != nil || ended.State != "SUCCEEDED" {
		t.Fatalf("finalize: %s %v", ended.State, e)
	}
	if _, e = s.Pool.Exec(ctx, `UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, a.Attempt.ID); e != nil {
		t.Fatal(e)
	}
	if v, e := s.Complete(ctx, successful(a)); e != nil || !v.Duplicate {
		t.Fatalf("committed completion rejected after expiry: %+v %v", v, e)
	}
}
func TestClaimReplayExpiryAndNewGeneration(t *testing.T) {
	s := testStore(t)
	w := publishFixture(t, s, false)
	submitFixture(t, s, w)
	runner := registerFixture(t, s, 1)
	ctx := context.Background()
	ready, e := s.ListReady(ctx)
	if e != nil {
		t.Fatal(e)
	}
	c := domain.ClaimCommand{SessionID: runner.ID, RequestID: "lost-reply", RequestedSlots: 1, Candidates: ready}
	first, e := s.Claim(ctx, c)
	if e != nil || len(first.Assignments) != 1 {
		t.Fatalf("first claim %v %+v", e, first)
	}
	c.Candidates = nil
	second, e := s.Claim(ctx, c)
	if e != nil || !second.Duplicate || len(second.Assignments) != 1 || first.Assignments[0].Attempt.ID != second.Assignments[0].Attempt.ID {
		t.Fatalf("claim replay %v %+v", e, second)
	}
	c.RequestedSlots = 2
	if _, e = s.Claim(ctx, c); e == nil {
		t.Fatal("changed retry parameters accepted")
	}
	c.RequestedSlots = 1
	old := first.Assignments[0]
	if _, e = s.Pool.Exec(ctx, `UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, old.Attempt.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Renew(ctx, runner.ID, old.Attempt.ID, old.Attempt.LeaseToken); e == nil {
		t.Fatal("expired lease renewed")
	}
	if e = s.ReapOne(ctx, old.Attempt.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Pool.Exec(ctx, `UPDATE invocations SET not_before=clock_timestamp()-interval '1 second' WHERE id=$1`, old.Invocation.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	replay, e := s.Claim(ctx, c)
	if e != nil || len(replay.Assignments) != 0 || len(replay.ExpiredAttemptIDs) != 1 {
		t.Fatalf("expired request allocated replacements %+v %v", replay, e)
	}
	next := claimFixture(t, s, runner, 1)
	if len(next.Assignments) != 1 || next.Assignments[0].Invocation.ID != old.Invocation.ID || next.Assignments[0].Attempt.ID == old.Attempt.ID {
		t.Fatal("retry identity incorrect")
	}
	if _, e = s.Complete(ctx, successful(old)); e == nil {
		t.Fatal("zombie completion accepted")
	}
}
func TestCancellationAndRetiredBootFence(t *testing.T) {
	s := testStore(t)
	w := publishFixture(t, s, false)
	run := submitFixture(t, s, w)
	runner := registerFixture(t, s, 1)
	batch := claimFixture(t, s, runner, 1)
	a := batch.Assignments[0]
	ctx := context.Background()
	if _, e := s.Started(ctx, runner.ID, a.Attempt.ID, a.Attempt.LeaseToken); e != nil {
		t.Fatal(e)
	}
	if _, e := s.CancelRun(ctx, run.ID, "test cancel"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Complete(ctx, successful(a)); e == nil {
		t.Fatal("cancelled run accepted first completion")
	}
	if e := s.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if e := s.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	ended, e := s.GetRun(ctx, run.ID)
	if e != nil || ended.State != "CANCELLED" {
		t.Fatalf("cancel cleanup %s %v", ended.State, e)
	}
	next := runner
	next.ID = ""
	next.BootID = domain.NewID()
	if _, e = s.Register(ctx, next); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Register(ctx, runner); e == nil {
		t.Fatal("old boot resurrected")
	}
}
func TestRecoveryRebuildAfterCommitBeforeProjection(t *testing.T) {
	s := testStore(t)
	w := publishFixture(t, s, false)
	submitFixture(t, s, w)
	q := scheduler.New(scheduler.Config{})
	p := projection.New(s, q, nil, projection.Hooks{})
	if e := p.Rebuild(context.Background()); e != nil {
		t.Fatal(e)
	}
	if q.Snapshot().ReadyTasks != 1 {
		t.Fatal("persisted task missing after recovery")
	}
	if e := p.Rebuild(context.Background()); e != nil {
		t.Fatal(e)
	}
	if q.Snapshot().ReadyTasks != 1 {
		t.Fatal("rebuild duplicated logical task")
	}
	ready, e := s.ListReady(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	q.Remove(ready[0].InvocationID, ready[0].Generation)
	if e = p.Rebuild(context.Background()); e != nil {
		t.Fatal(e)
	}
	if q.Snapshot().ReadyTasks != 1 {
		t.Fatal("missing already-delivered Ready index was not repaired")
	}
}
