// Package scheduler maintains disposable, resource-aware scheduling indexes.
// PostgreSQL remains the authority for execution ownership.
package scheduler

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRunnerUnavailable  = errors.New("runner session is not active")
	ErrReservationUnknown = errors.New("reservation is unknown or already resolved")
)

type Config struct {
	AffinityDelay   time.Duration
	AgingInterval   time.Duration
	MaxWait         time.Duration
	ReservationTTL  time.Duration
	DisableStealing bool
	Now             func() time.Time `json:"-"`
}

type Task struct {
	ID                string
	RunID             string
	Version           int64
	FunctionID        string
	FunctionVersion   string
	AffinityKey       string
	Runtime           string
	Priority          int
	ReadyAt           time.Time
	Deadline          time.Time
	CPUMillis         int64
	MemoryBytes       int64
	EstimatedDuration time.Duration
}

type Runner struct {
	ID                   string
	SessionID            string
	Active               bool
	Runtimes             []string
	CachedKeys           []string
	AvailableSlots       int
	AvailableCPUMillis   int64
	AvailableMemoryBytes int64
}

type Reservation struct {
	ID        string
	RunnerID  string
	SessionID string
	Tasks     []Task
	ExpiresAt time.Time
}

type Snapshot struct {
	ReadyTasks     int
	ReservedTasks  int
	ActiveRunners  int
	AvailableSlots int
	OldestReadyAge time.Duration
	ReadyWork      time.Duration
	ByRunner       map[string]int
	ByFunction     map[string]int
}

const (
	queued uint32 = iota + 1
	reserved
	done
)

type entry struct {
	task              Task
	owner             string
	sequence          uint64
	effectivePriority int
	state             atomic.Uint32
}

type priorityHeap []*entry

func (h priorityHeap) Len() int { return len(h) }
func (h priorityHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.effectivePriority != b.effectivePriority {
		return a.effectivePriority > b.effectivePriority
	}
	if !a.task.Deadline.Equal(b.task.Deadline) {
		if a.task.Deadline.IsZero() {
			return false
		}
		if b.task.Deadline.IsZero() {
			return true
		}
		return a.task.Deadline.Before(b.task.Deadline)
	}
	if !a.task.ReadyAt.Equal(b.task.ReadyAt) {
		return a.task.ReadyAt.Before(b.task.ReadyAt)
	}
	return a.sequence < b.sequence
}
func (h priorityHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *priorityHeap) Push(v any)   { *h = append(*h, v.(*entry)) }
func (h *priorityHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

type shard struct {
	// dispatch serializes reservations for this requesting runner, not the pool.
	dispatch   sync.Mutex
	mu         sync.Mutex
	runner     Runner
	items      priorityHeap
	lastAged   time.Time
	heldSlots  int
	heldCPU    int64
	heldMemory int64
}

type pending struct {
	value     Reservation
	entries   []*entry
	requester *shard
}

type Scheduler struct {
	configMu  sync.RWMutex
	config    Config
	mu        sync.RWMutex // protects registry and the version index, never database I/O
	shards    map[string]*shard
	entries   map[string]*entry
	global    *shard
	pendingMu sync.Mutex
	pending   map[string]*pending
	notifyMu  sync.Mutex
	changed   chan struct{}
	sequence  atomic.Uint64
}

func New(config Config) *Scheduler {
	config = normalizeConfig(config)
	return &Scheduler{config: config, shards: make(map[string]*shard), entries: make(map[string]*entry), global: &shard{}, pending: make(map[string]*pending), changed: make(chan struct{})}
}

func normalizeConfig(config Config) Config {
	if config.AffinityDelay <= 0 {
		config.AffinityDelay = 20 * time.Millisecond
	}
	if config.AgingInterval <= 0 {
		config.AgingInterval = time.Second
	}
	if config.MaxWait <= 0 {
		config.MaxWait = 10 * time.Second
	}
	if config.ReservationTTL <= 0 {
		config.ReservationTTL = 30 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return config
}

// Config returns a policy snapshot. The clock is never serialized as policy.
func (s *Scheduler) Config() Config {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.config
}

// UpdateConfig applies a validated policy without replacing disposable indexes.
func (s *Scheduler) UpdateConfig(config Config) error {
	if config.AffinityDelay < 0 || config.AgingInterval < 0 || config.MaxWait < 0 || config.ReservationTTL < 0 {
		return errors.New("scheduling policy durations must be nonnegative")
	}
	s.configMu.Lock()
	if config.Now == nil {
		config.Now = s.config.Now
	}
	s.config = normalizeConfig(config)
	s.configMu.Unlock()
	for _, q := range s.shardSnapshot() {
		q.mu.Lock()
		q.lastAged = time.Time{}
		q.mu.Unlock()
	}
	s.global.mu.Lock()
	s.global.lastAged = time.Time{}
	s.global.mu.Unlock()
	s.signal()
	return nil
}

func (s *Scheduler) signal() {
	s.notifyMu.Lock()
	close(s.changed)
	s.changed = make(chan struct{})
	s.notifyMu.Unlock()
}

// UpsertRunner refreshes authoritative available resources. Call before each Acquire.
func (s *Scheduler) UpsertRunner(view Runner) {
	view.Runtimes = append([]string(nil), view.Runtimes...)
	view.CachedKeys = append([]string(nil), view.CachedKeys...)
	s.mu.Lock()
	q := s.shards[view.ID]
	if q == nil {
		q = &shard{}
		s.shards[view.ID] = q
	}
	s.mu.Unlock()
	q.dispatch.Lock()
	q.mu.Lock()
	q.runner = view
	q.mu.Unlock()
	q.dispatch.Unlock()
	s.signal()
}

// RemoveRunner retains its READY entries for immediate work stealing.
func (s *Scheduler) RemoveRunner(id string) {
	s.mu.RLock()
	q := s.shards[id]
	s.mu.RUnlock()
	if q != nil {
		q.mu.Lock()
		q.runner.Active = false
		q.mu.Unlock()
		s.signal()
	}
}

// Expiry of an old boot must never deactivate a newly registered boot that
// happens to use the same stable Runner ID.
func (s *Scheduler) RemoveSession(id, sessionID string) {
	s.mu.RLock()
	q := s.shards[id]
	s.mu.RUnlock()
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.runner.SessionID == sessionID {
		q.runner.Active = false
	}
	q.mu.Unlock()
	s.signal()
}

func compatible(runtime string, runtimes []string) bool {
	for _, candidate := range runtimes {
		if candidate == runtime {
			return true
		}
	}
	return false
}
func cached(key string, keys []string) bool {
	for _, candidate := range keys {
		if candidate == key {
			return true
		}
	}
	return false
}

func (s *Scheduler) shardSnapshot() map[string]*shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*shard, len(s.shards))
	for id, q := range s.shards {
		result[id] = q
	}
	return result
}

func (s *Scheduler) owner(task Task) (string, *shard) {
	shards := s.shardSnapshot()
	type candidate struct {
		id    string
		q     *shard
		score uint64
		warm  bool
	}
	var selected candidate
	for id, q := range shards {
		q.mu.Lock()
		view := q.runner
		q.mu.Unlock()
		if !view.Active || !compatible(task.Runtime, view.Runtimes) {
			continue
		}
		hash := sha256.Sum256([]byte(task.AffinityKey + "\x00" + id))
		item := candidate{id: id, q: q, score: binary.BigEndian.Uint64(hash[:8]), warm: cached(task.AffinityKey, view.CachedKeys)}
		if selected.q == nil || (item.warm && !selected.warm) || (item.warm == selected.warm && (item.score > selected.score || (item.score == selected.score && item.id < selected.id))) {
			selected = item
		}
	}
	if selected.q == nil {
		return "", s.global
	}
	return selected.id, selected.q
}

// Enqueue is idempotent for (ID, Version). A newer version invalidates old indexes.
func (s *Scheduler) Enqueue(task Task) error {
	if task.ID == "" || task.Version < 0 || task.Runtime == "" || task.CPUMillis < 0 || task.MemoryBytes < 0 || task.Priority < 0 || task.Priority > 100 {
		return errors.New("invalid scheduling task")
	}
	config := s.Config()
	now := config.Now()
	if task.ReadyAt.IsZero() {
		task.ReadyAt = now
	}
	owner, q := s.owner(task)
	e := &entry{task: task, owner: owner, sequence: s.sequence.Add(1), effectivePriority: task.Priority}
	e.state.Store(queued)
	s.mu.Lock()
	if previous := s.entries[task.ID]; previous != nil {
		if previous.task.Version >= task.Version {
			s.mu.Unlock()
			return nil
		}
		previous.state.Store(done)
	}
	s.entries[task.ID] = e
	s.mu.Unlock()
	q.mu.Lock()
	heap.Push(&q.items, e)
	q.mu.Unlock()
	s.signal()
	return nil
}

// Remove invalidates only the specified or an older READY version.
// ReconcileReady is reserved for authoritative database snapshots. Ordinary
// outbox replays use Enqueue so a completed generation is not resurrected.
func (s *Scheduler) ReconcileReady(task Task) error {
	s.mu.Lock()
	previous := s.entries[task.ID]
	if previous != nil && previous.task.Version == task.Version && previous.state.Load() == done {
		delete(s.entries, task.ID)
	}
	s.mu.Unlock()
	return s.Enqueue(task)
}

func (s *Scheduler) Remove(id string, version int64) {
	s.mu.RLock()
	e := s.entries[id]
	s.mu.RUnlock()
	if e != nil && e.task.Version <= version {
		e.state.Store(done)
		s.signal()
	}
}

// Forget drops terminal-run tombstones after the caller's outbox retention boundary.
func (s *Scheduler) Forget(ids ...string) {
	s.mu.Lock()
	for _, id := range ids {
		if e := s.entries[id]; e != nil {
			e.state.Store(done)
			delete(s.entries, id)
		}
	}
	s.mu.Unlock()
	s.signal()
}

func (s *Scheduler) age(q *shard, now time.Time) {
	config := s.Config()
	if !q.lastAged.IsZero() && now.Sub(q.lastAged) < config.AgingInterval {
		return
	}
	live := q.items[:0]
	for _, e := range q.items {
		if e.state.Load() != queued {
			continue
		}
		age := now.Sub(e.task.ReadyAt) / config.AgingInterval
		if age < 0 {
			age = 0
		}
		e.effectivePriority = e.task.Priority + int(age)
		if e.effectivePriority > 100 {
			e.effectivePriority = 100
		}
		live = append(live, e)
	}
	for i := len(live); i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = live
	heap.Init(&q.items)
	q.lastAged = now
}

type budget struct {
	slots       int
	cpu, memory int64
}

func fits(t Task, view Runner, b budget) bool {
	return b.slots > 0 && compatible(t.Runtime, view.Runtimes) && t.CPUMillis <= b.cpu && t.MemoryBytes <= b.memory
}

func (s *Scheduler) pop(q *shard, view Runner, remaining budget, stealing bool, now time.Time) *entry {
	config := s.Config()
	q.mu.Lock()
	defer q.mu.Unlock()
	s.age(q, now)
	skipped := make([]*entry, 0, 8)
	var selected *entry
	// A bounded scan prevents one oversized or delayed head from blocking a shard.
	for scanned := 0; q.items.Len() > 0 && scanned < 64; scanned++ {
		e := heap.Pop(&q.items).(*entry)
		if e.state.Load() != queued {
			continue
		}
		if (stealing && q.runner.Active && now.Before(e.task.ReadyAt.Add(config.AffinityDelay))) || !fits(e.task, view, remaining) {
			skipped = append(skipped, e)
			continue
		}
		if e.state.CompareAndSwap(queued, reserved) {
			selected = e
			break
		}
	}
	for _, e := range skipped {
		heap.Push(&q.items, e)
	}
	return selected
}

func (s *Scheduler) Reserve(runnerID string, limit int) (Reservation, error) {
	s.ReapExpired()
	s.mu.RLock()
	requester := s.shards[runnerID]
	s.mu.RUnlock()
	if requester == nil {
		return Reservation{}, ErrRunnerUnavailable
	}
	requester.dispatch.Lock()
	defer requester.dispatch.Unlock()
	requester.mu.Lock()
	view := requester.runner
	remaining := budget{slots: view.AvailableSlots - requester.heldSlots, cpu: view.AvailableCPUMillis - requester.heldCPU, memory: view.AvailableMemoryBytes - requester.heldMemory}
	requester.mu.Unlock()
	if !view.Active {
		return Reservation{}, ErrRunnerUnavailable
	}
	if limit <= 0 || remaining.slots <= 0 {
		return Reservation{}, nil
	}
	if limit < remaining.slots {
		remaining.slots = limit
	}
	shards := s.shardSnapshot()
	ids := make([]string, 0, len(shards))
	for id := range shards {
		if id != runnerID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	config := s.Config()
	now := config.Now()
	selected := make([]*entry, 0, remaining.slots)
	for remaining.slots > 0 {
		e := s.pop(requester, view, remaining, false, now)
		if e == nil {
			e = s.pop(s.global, view, remaining, false, now)
		}
		if e == nil {
			// Rotate victims per reservation to avoid permanently favoring one shard.
			offset := int(s.sequence.Load() % uint64(len(ids)+1))
			for i := 0; i < len(ids); i++ {
				victim := shards[ids[(i+offset)%len(ids)]]
				if config.DisableStealing {
					victim.mu.Lock()
					active := victim.runner.Active
					victim.mu.Unlock()
					if active {
						continue
					}
				}
				if e = s.pop(victim, view, remaining, true, now); e != nil {
					break
				}
			}
		}
		if e == nil {
			break
		}
		selected = append(selected, e)
		remaining.slots--
		remaining.cpu -= e.task.CPUMillis
		remaining.memory -= e.task.MemoryBytes
	}
	if len(selected) == 0 {
		return Reservation{}, nil
	}
	value := Reservation{ID: fmt.Sprintf("reservation-%d", s.sequence.Add(1)), RunnerID: view.ID, SessionID: view.SessionID, ExpiresAt: now.Add(config.ReservationTTL)}
	requester.mu.Lock()
	requester.heldSlots += len(selected)
	for _, e := range selected {
		value.Tasks = append(value.Tasks, e.task)
		requester.heldCPU += e.task.CPUMillis
		requester.heldMemory += e.task.MemoryBytes
	}
	requester.mu.Unlock()
	s.pendingMu.Lock()
	s.pending[value.ID] = &pending{value: value, entries: selected, requester: requester}
	s.pendingMu.Unlock()
	return value, nil
}

func (s *Scheduler) resolve(id string, claimed map[string]bool) error {
	s.pendingMu.Lock()
	p := s.pending[id]
	delete(s.pending, id)
	s.pendingMu.Unlock()
	if p == nil {
		return ErrReservationUnknown
	}
	p.requester.mu.Lock()
	p.requester.heldSlots -= len(p.entries)
	for _, e := range p.entries {
		p.requester.heldCPU -= e.task.CPUMillis
		p.requester.heldMemory -= e.task.MemoryBytes
	}
	p.requester.mu.Unlock()
	for _, e := range p.entries {
		if claimed[e.task.ID] {
			e.state.CompareAndSwap(reserved, done)
			continue
		}
		if !e.state.CompareAndSwap(reserved, queued) {
			continue
		}
		s.mu.RLock()
		q := s.shards[e.owner]
		s.mu.RUnlock()
		if q == nil {
			q = s.global
		}
		q.mu.Lock()
		heap.Push(&q.items, e)
		q.mu.Unlock()
	}
	s.signal()
	return nil
}

// Commit resolves all candidates. Unclaimed candidates return to their owner shard.
func (s *Scheduler) Commit(id string, claimedIDs []string) error {
	claimed := make(map[string]bool, len(claimedIDs))
	for _, taskID := range claimedIDs {
		claimed[taskID] = true
	}
	return s.resolve(id, claimed)
}
func (s *Scheduler) Abort(id string) error { return s.resolve(id, nil) }

func (s *Scheduler) ReapExpired() {
	now := s.Config().Now()
	s.pendingMu.Lock()
	var ids []string
	for id, p := range s.pending {
		if !p.value.ExpiresAt.After(now) {
			ids = append(ids, id)
		}
	}
	s.pendingMu.Unlock()
	for _, id := range ids {
		_ = s.Abort(id)
	}
}

// Wait bounds long polling and also wakes when a locality grace period expires.
func (s *Scheduler) Wait(ctx context.Context, runnerID string, timeout time.Duration) error {
	config := s.Config()
	if timeout <= 0 {
		return nil
	}
	if timeout > config.MaxWait {
		timeout = config.MaxWait
	}
	s.notifyMu.Lock()
	changed := s.changed
	s.notifyMu.Unlock()
	s.mu.RLock()
	q := s.shards[runnerID]
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.mu.RUnlock()
	if q == nil {
		return ErrRunnerUnavailable
	}
	q.mu.Lock()
	view := q.runner
	remaining := budget{view.AvailableSlots - q.heldSlots, view.AvailableCPUMillis - q.heldCPU, view.AvailableMemoryBytes - q.heldMemory}
	q.mu.Unlock()
	if !view.Active {
		return ErrRunnerUnavailable
	}
	now := config.Now()
	for _, e := range entries {
		if e.state.Load() != queued || !fits(e.task, view, remaining) {
			continue
		}
		if e.owner == "" || e.owner == runnerID {
			return nil
		}
		s.mu.RLock()
		owner := s.shards[e.owner]
		s.mu.RUnlock()
		active := false
		if owner != nil {
			owner.mu.Lock()
			active = owner.runner.Active
			owner.mu.Unlock()
		}
		if config.DisableStealing && active {
			continue
		}
		stealAfter := e.task.ReadyAt.Add(config.AffinityDelay)
		if !active || !now.Before(stealAfter) {
			return nil
		}
		if delay := stealAfter.Sub(now); delay < timeout {
			timeout = delay
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timer.C:
		return nil
	}
}

func (s *Scheduler) Snapshot() Snapshot {
	now := s.Config().Now()
	result := Snapshot{ByRunner: make(map[string]int), ByFunction: make(map[string]int)}
	s.mu.RLock()
	entries := make([]*entry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e)
	}
	s.mu.RUnlock()
	for _, e := range entries {
		switch e.state.Load() {
		case queued:
			result.ReadyTasks++
			result.ByRunner[e.owner]++
			result.ByFunction[e.task.FunctionID+"@"+e.task.FunctionVersion]++
			result.ReadyWork += e.task.EstimatedDuration
			if age := now.Sub(e.task.ReadyAt); age > result.OldestReadyAge {
				result.OldestReadyAge = age
			}
		case reserved:
			result.ReservedTasks++
		}
	}
	for _, q := range s.shardSnapshot() {
		q.mu.Lock()
		if q.runner.Active {
			result.ActiveRunners++
			free := q.runner.AvailableSlots - q.heldSlots
			if free > 0 {
				result.AvailableSlots += free
			}
		}
		q.mu.Unlock()
	}
	return result
}
