// Package autoscaler reconciles a small Docker Runner pool from live demand.
package autoscaler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

type Config struct {
	Paused             bool
	MinRunners         int
	MaxRunners         int
	SlotsPerRunner     int
	TargetUtilization  float64
	QueueBudget        time.Duration
	ScaleUpConsecutive int
	ScaleDownWindow    time.Duration
	StartupTimeout     time.Duration
	Now                func() time.Time `json:"-"`
}
type RunnerLoad struct {
	ID, SessionID, State                         string
	ActiveAttempts, PendingCompletions, WarmKeys int
}
type PoolSnapshot struct {
	LeasedTasks, RunningTasks, CompletingTasks                            int
	ActiveRunners, StartingRunners, TotalSlots, OccupiedSlots, ReadyTasks int
	ReadyWork, RunningWork, QueueBudget, OldestReady                      time.Duration
	ArrivalRate, MeanExecutionSeconds                                     float64
	RunSLORisk                                                            bool
	Runners                                                               []RunnerLoad
}
type RunnerSpec struct {
	ID    string
	Slots int
}
type ManagedRunner struct {
	ID, ContainerID, State string
	CreatedAt              time.Time
}
type SnapshotProvider interface {
	PoolSnapshot(context.Context) (PoolSnapshot, error)
}
type PolicySetter interface {
	SetDraining(context.Context, string, string) error
}
type Driver interface {
	List(context.Context) ([]ManagedRunner, error)
	Create(context.Context, RunnerSpec) (ManagedRunner, error)
	Remove(context.Context, string) error
}
type Decision struct {
	DesiredRunners, ExistingRunners, StartingRunners int
	Created, Draining, Removed                       []string
	EstimatedDrainTime                               time.Duration
	Reason                                           string
}
type Controller struct {
	mu        sync.Mutex
	config    Config
	provider  SnapshotProvider
	policies  PolicySetter
	driver    Driver
	highTicks int
	lowSince  time.Time
	sequence  uint64
}

func New(config Config, provider SnapshotProvider, policies PolicySetter, driver Driver) *Controller {
	return &Controller{config: normalizeConfig(config), provider: provider, policies: policies, driver: driver}
}

func normalizeConfig(config Config) Config {
	if config.MinRunners <= 0 {
		config.MinRunners = 1
	}
	if config.MaxRunners <= 0 {
		config.MaxRunners = 4
	}
	if config.MaxRunners < config.MinRunners {
		config.MaxRunners = config.MinRunners
	}
	if config.SlotsPerRunner <= 0 {
		config.SlotsPerRunner = 2
	}
	if config.TargetUtilization <= 0 || config.TargetUtilization >= 1 {
		config.TargetUtilization = .7
	}
	if config.QueueBudget <= 0 {
		config.QueueBudget = 500 * time.Millisecond
	}
	if config.ScaleUpConsecutive <= 0 {
		config.ScaleUpConsecutive = 2
	}
	if config.ScaleDownWindow <= 0 {
		config.ScaleDownWindow = 60 * time.Second
	}
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = 60 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return config
}

func (c *Controller) Config() Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config
}

// UpdateConfig atomically applies policy between reconciliations. Existing
// containers retain their resource/slot spec; new containers use the new policy.
func (c *Controller) UpdateConfig(config Config) error {
	if config.SlotsPerRunner > 128 {
		return errors.New("runner slots exceed the supported maximum of 128")
	}
	if config.MinRunners > 0 && config.MaxRunners > 0 && config.MaxRunners < config.MinRunners {
		return errors.New("maximum runners must be at least minimum runners")
	}
	if config.MinRunners < 0 || config.MaxRunners < 0 || config.SlotsPerRunner < 0 ||
		config.QueueBudget < 0 || config.ScaleUpConsecutive < 0 || config.ScaleDownWindow < 0 ||
		config.StartupTimeout < 0 || math.IsNaN(config.TargetUtilization) ||
		math.IsInf(config.TargetUtilization, 0) || config.TargetUtilization < 0 || config.TargetUtilization >= 1 {
		return errors.New("invalid autoscaling policy")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if config.Now == nil {
		config.Now = c.config.Now
	}
	config = normalizeConfig(config)
	if config.MaxRunners < config.MinRunners {
		return errors.New("maximum runners must be at least minimum runners")
	}
	c.config = config
	c.highTicks = 0
	c.lowSince = time.Time{}
	return nil
}

func (c *Controller) target(s PoolSnapshot) (int, time.Duration) {
	budget := s.QueueBudget
	if budget <= 0 {
		budget = c.config.QueueBudget
	}
	if budget < time.Millisecond {
		budget = time.Millisecond
	}
	var drain time.Duration
	if s.TotalSlots > 0 {
		drain = (s.ReadyWork + s.RunningWork) / time.Duration(s.TotalSlots)
	} else if s.ReadyTasks > 0 || s.ReadyWork > 0 {
		drain = time.Duration(math.MaxInt64)
	}
	slots := int(math.Ceil(math.Max(0, s.ArrivalRate) * math.Max(0, s.MeanExecutionSeconds) / c.config.TargetUtilization))
	// Additional capacity cannot accelerate an already-running attempt. Backlog
	// pressure is applied only when READY work actually exists.
	if s.ReadyTasks > 0 || s.ReadyWork > 0 {
		needed := int(math.Ceil((s.ReadyWork + s.RunningWork).Seconds() / budget.Seconds()))
		if needed > slots {
			slots = needed
		}
	}
	if s.OccupiedSlots > slots {
		slots = s.OccupiedSlots
	}
	desired := int(math.Ceil(float64(slots) / float64(c.config.SlotsPerRunner)))
	if desired < c.config.MinRunners {
		desired = c.config.MinRunners
	}
	if desired > c.config.MaxRunners {
		desired = c.config.MaxRunners
	}
	return desired, drain
}

// Reconcile performs one bounded control-loop iteration. The caller normally
// invokes it once per second. Snapshot failure never triggers container deletion.
func (c *Controller) Reconcile(ctx context.Context) (Decision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.provider == nil || c.driver == nil || c.policies == nil {
		return Decision{}, errors.New("autoscaler dependencies are required")
	}
	snapshot, err := c.provider.PoolSnapshot(ctx)
	if err != nil {
		return Decision{}, fmt.Errorf("pool snapshot: %w", err)
	}
	if snapshot.ActiveRunners < 0 || snapshot.StartingRunners < 0 || snapshot.TotalSlots < 0 ||
		snapshot.OccupiedSlots < 0 || snapshot.ReadyTasks < 0 || snapshot.ReadyWork < 0 ||
		snapshot.RunningWork < 0 || snapshot.ArrivalRate < 0 || snapshot.MeanExecutionSeconds < 0 ||
		math.IsNaN(snapshot.ArrivalRate) || math.IsNaN(snapshot.MeanExecutionSeconds) ||
		math.IsInf(snapshot.ArrivalRate, 0) || math.IsInf(snapshot.MeanExecutionSeconds, 0) {
		return Decision{}, errors.New("invalid pool demand snapshot")
	}
	managed, err := c.driver.List(ctx)
	if err != nil {
		return Decision{}, fmt.Errorf("discover managed runners: %w", err)
	}
	now := c.config.Now()
	loads := make(map[string]RunnerLoad, len(snapshot.Runners))
	for _, r := range snapshot.Runners {
		loads[r.ID] = r
	}
	decision := Decision{Reason: "capacity within limits"}
	starting := 0
	containers := make(map[string]ManagedRunner, len(managed))
	for _, container := range managed {
		containers[container.ID] = container
		load, known := loads[container.ID]
		if known && (load.State == "DRAINING" || load.State == "OFFLINE") && load.ActiveAttempts == 0 && load.PendingCompletions == 0 {
			if err = c.driver.Remove(ctx, container.ContainerID); err != nil {
				return decision, fmt.Errorf("remove drained runner %s: %w", container.ID, err)
			}
			decision.Removed = append(decision.Removed, container.ID)
			continue
		}
		if !known {
			if container.State == "exited" || container.State == "dead" ||
				(!container.CreatedAt.IsZero() && now.Sub(container.CreatedAt) > c.config.StartupTimeout) {
				if err = c.driver.Remove(ctx, container.ContainerID); err != nil {
					return decision, fmt.Errorf("remove unregistered runner %s: %w", container.ID, err)
				}
				decision.Removed = append(decision.Removed, container.ID)
				continue
			}
			starting++
		}
	}
	if snapshot.StartingRunners > starting {
		starting = snapshot.StartingRunners
	}
	current := snapshot.ActiveRunners + starting
	if c.config.Paused {
		decision.ExistingRunners = current
		decision.DesiredRunners = current
		decision.StartingRunners = starting
		decision.Reason = "new capacity paused; explicit drains still complete"
		return decision, nil
	}
	desired, drain := c.target(snapshot)
	decision.DesiredRunners = desired
	decision.ExistingRunners = current
	decision.StartingRunners = starting
	decision.EstimatedDrainTime = drain
	if desired > current {
		c.lowSince = time.Time{}
		c.highTicks++
		budget := snapshot.QueueBudget
		if budget <= 0 {
			budget = c.config.QueueBudget
		}
		emergency := (snapshot.ReadyTasks > 0 || snapshot.ReadyWork > 0) &&
			(snapshot.TotalSlots == 0 || snapshot.RunSLORisk || snapshot.OldestReady >= budget)
		if c.highTicks < c.config.ScaleUpConsecutive && !emergency {
			decision.Reason = "waiting for sustained demand"
			return decision, nil
		}
		for count := current; count < desired; count++ {
			c.sequence++
			id := fmt.Sprintf("runner-%d-%d", now.UnixNano(), c.sequence)
			created, createErr := c.driver.Create(ctx, RunnerSpec{ID: id, Slots: c.config.SlotsPerRunner})
			if createErr != nil {
				return decision, fmt.Errorf("create runner: %w", createErr)
			}
			decision.Created = append(decision.Created, created.ID)
		}
		c.highTicks = 0
		decision.Reason = "live queue or sustained arrival pressure"
		return decision, nil
	}
	c.highTicks = 0
	if desired >= current || starting > 0 {
		c.lowSince = time.Time{}
		return decision, nil
	}
	if c.lowSince.IsZero() {
		c.lowSince = now
		decision.Reason = "waiting for low-load window"
		return decision, nil
	}
	if now.Sub(c.lowSince) < c.config.ScaleDownWindow {
		decision.Reason = "waiting for low-load window"
		return decision, nil
	}
	var candidates []RunnerLoad
	for _, load := range snapshot.Runners {
		if load.State == "ACTIVE" && load.ActiveAttempts == 0 && load.PendingCompletions == 0 {
			if _, managedByUs := containers[load.ID]; managedByUs {
				candidates = append(candidates, load)
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].WarmKeys != candidates[j].WarmKeys {
			return candidates[i].WarmKeys < candidates[j].WarmKeys
		}
		return candidates[i].ID < candidates[j].ID
	})
	// Drain one runner at a time; it is removed only after a later authoritative
	// snapshot confirms no outstanding execution or completion report.
	if len(candidates) > 0 {
		candidate := candidates[0]
		if err = c.policies.SetDraining(ctx, candidate.ID, candidate.SessionID); err != nil {
			return decision, fmt.Errorf("drain runner: %w", err)
		}
		decision.Draining = append(decision.Draining, candidate.ID)
		decision.Reason = "sustained low load; waiting for drain acknowledgement"
	}
	return decision, nil
}

func (c *Controller) Tick(ctx context.Context) (Decision, error) { return c.Reconcile(ctx) }
