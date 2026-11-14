// Package bootstrap assembles modules and owns process-level resource lifetimes.
package bootstrap

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	dockeradapter "fluxforge-control/internal/adapters/docker"
	minioadapter "fluxforge-control/internal/adapters/minio"
	"fluxforge-control/internal/adapters/postgres"
	"fluxforge-control/internal/autoscaler"
	"fluxforge-control/internal/catalog"
	"fluxforge-control/internal/config"
	"fluxforge-control/internal/execution"
	"fluxforge-control/internal/objects"
	"fluxforge-control/internal/projection"
	"fluxforge-control/internal/recovery"
	"fluxforge-control/internal/runnerregistry"
	"fluxforge-control/internal/scheduler"
	"fluxforge-control/internal/telemetry"
	transport "fluxforge-control/internal/transport/grpc"
	objectgrpc "fluxforge-control/internal/transport/objectgrpc"
	"fluxforge-control/internal/workflow"
	pb "fluxforge-control/pkg/api/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func auth(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token != "" {
			md, _ := metadata.FromIncomingContext(ctx)
			values := md.Get("authorization")
			if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+token)) != 1 {
				return nil, status.Error(codes.Unauthenticated, "valid bearer token required")
			}
		}
		return handler(ctx, req)
	}
}
func Run(ctx context.Context, c config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pool, e := pgxpool.New(ctx, c.DSN)
	if e != nil {
		return e
	}
	defer pool.Close()
	startup, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	if e = pool.Ping(startup); e != nil {
		return fmt.Errorf("postgres: %w", e)
	}
	if e = postgres.Migrate(startup, pool); e != nil {
		return e
	}
	controller, e := pool.Acquire(startup)
	if e != nil {
		return e
	}
	defer controller.Release()
	var locked bool
	if e = controller.QueryRow(startup, `SELECT pg_try_advisory_lock(721465982)`).Scan(&locked); e != nil {
		return e
	}
	if !locked {
		return errors.New("another FluxForge controller owns this database")
	}
	defer controller.Exec(context.Background(), `SELECT pg_advisory_unlock(721465982)`)
	store := postgres.New(pool)
	if e = store.InitPolicies(startup); e != nil {
		return e
	}
	objectRepo := &postgres.ObjectRepository{Pool: pool}
	if e = objectRepo.Init(startup); e != nil {
		return e
	}
	backend, e := minioadapter.New(minioadapter.Config{Endpoint: c.MinIOEndpoint, PublicEndpoint: c.MinIOPublicEndpoint, AccessKey: c.MinIOAccessKey, SecretKey: c.MinIOSecretKey, Bucket: c.MinIOBucket})
	if e != nil {
		return e
	}
	if e = backend.Ensure(startup); e != nil {
		return fmt.Errorf("minio: %w", e)
	}
	objectService := objects.New(objectRepo, backend, c.MinIOBucket)
	trace, e := telemetry.NewTracing(ctx, c.OTLPEndpoint)
	if e != nil {
		return e
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if e := trace.Shutdown(shutdown); e != nil {
			slog.Warn("trace shutdown", "error", e)
		}
	}()
	metrics := telemetry.New(telemetry.Config{})
	queue := scheduler.New(scheduler.Config{AffinityDelay: c.AffinityDelay})
	projector := projection.New(store, queue, metrics, projection.Hooks{Invocation: trace.Invocation, Run: trace.Run})
	leases := execution.New(store, queue, projector, objectService, execution.Hooks{Started: trace.Started, Completed: trace.Completed})
	runners := runnerregistry.New(store)
	recoverer := recovery.New(store, projector)
	if e = recoverer.Recover(startup); e != nil {
		return e
	}
	sessions, e := runners.ListSessions(startup)
	if e != nil {
		return e
	}
	for _, r := range sessions {
		if r.State == "ACTIVE" {
			if _, e = leases.RefreshRunner(startup, r.ID); e != nil {
				return e
			}
		}
	}
	view := &poolView{store: store, runners: runners, metrics: metrics}
	api := &transport.Server{Catalog: catalog.New(store, objectService), Workflow: workflow.New(store, objectService, projector), Execution: leases, Runners: runners, Metrics: metrics, Queue: queue, Policies: store, Pool: view, RuntimeDigest: c.RuntimeDigest}
	api.ValidateScheduling = func(b []byte) error {
		next := queue.Config()
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if e := decoder.Decode(&next); e != nil {
			return e
		}
		return scheduler.New(queue.Config()).UpdateConfig(next)
	}
	api.ApplyScheduling = func(b []byte) error {
		next := queue.Config()
		if e := json.Unmarshal(b, &next); e != nil {
			return e
		}
		return queue.UpdateConfig(next)
	}
	if e = restorePolicy(startup, store, "scheduling", queue.Config(), api.ApplyScheduling); e != nil {
		return e
	}
	public := grpc.NewServer(grpc.UnaryInterceptor(auth(c.PublicToken)), grpc.MaxRecvMsgSize(4<<20))
	internal := grpc.NewServer(grpc.UnaryInterceptor(auth(c.InternalToken)), grpc.MaxRecvMsgSize(4<<20))
	defer public.Stop()
	defer internal.Stop()
	pb.RegisterCatalogServiceServer(public, api)
	pb.RegisterWorkflowServiceServer(public, api)
	pb.RegisterObservabilityServiceServer(public, api)
	pb.RegisterObjectServiceServer(public, &objectgrpc.Server{Service: objectService, Internal: false})
	pb.RegisterRunnerQueueServiceServer(internal, api)
	pb.RegisterQueueAdminServiceServer(internal, api)
	pb.RegisterObjectServiceServer(internal, &objectgrpc.Server{Service: objectService, Internal: true})
	pl, e := net.Listen("tcp", c.PublicAddr)
	if e != nil {
		return e
	}
	defer pl.Close()
	il, e := net.Listen("tcp", c.InternalAddr)
	if e != nil {
		return e
	}
	defer il.Close()
	hl, e := net.Listen("tcp", c.HTTPAddr)
	if e != nil {
		return e
	}
	defer hl.Close()
	var ready atomic.Bool
	ready.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pong!\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", 503)
			return
		}
		probe, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if e := pool.Ping(probe); e != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.Handle("/metrics", metrics)
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	errs := make(chan error, 4)
	var workers sync.WaitGroup
	launch := func(fn func()) { workers.Add(1); go func() { defer workers.Done(); fn() }() }
	launch(func() { projector.Run(ctx) })
	launch(func() { recoverer.Run(ctx) })
	launch(func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probe, cancel := context.WithTimeout(ctx, time.Second)
				var n int
				e := controller.QueryRow(probe, "SELECT 1").Scan(&n)
				cancel()
				if e != nil {
					ready.Store(false)
					select {
					case errs <- fmt.Errorf("controller fencing connection lost: %w", e):
					default:
					}
					return
				}
				// Reflect persistent health/policy changes even when an offline
				// Runner never sends another Heartbeat/Acquire request.
				profiles, profileErr := runners.ListSessions(ctx)
				if profileErr == nil {
					for _, r := range profiles {
						if r.State != "ACTIVE" || r.Policy != "ACTIVE" || time.Since(r.LastSeen) > store.HeartbeatTimeout {
							queue.RemoveSession(r.RunnerID, r.ID)
						}
					}
				}
			}
		}
	})
	if c.Autoscaling {
		driver, e := dockeradapter.New(dockeradapter.Config{Image: c.RunnerImage, Network: c.Network, Project: c.Project, Environment: map[string]string{"QUEUE_ADDR": "control-plane:9091", "FF_INTERNAL_TOKEN": c.InternalToken, "FF_RUNTIME_DIGEST": c.RuntimeDigest, "FF_WORK_DIR": "/tmp/fluxforge"}})
		if e != nil {
			return e
		}
		scaler := autoscaler.New(autoscaler.Config{MinRunners: c.MinRunners, MaxRunners: c.MaxRunners, SlotsPerRunner: c.SlotsPerRunner}, view, runners, driver)
		view.driver = driver
		api.ValidateAutoscaling = func(b []byte) error {
			next := scaler.Config()
			decoder := json.NewDecoder(bytes.NewReader(b))
			decoder.DisallowUnknownFields()
			if e := decoder.Decode(&next); e != nil {
				return e
			}
			return autoscaler.New(scaler.Config(), view, runners, driver).UpdateConfig(next)
		}
		api.ApplyAutoscaling = func(b []byte) error {
			next := scaler.Config()
			if e := json.Unmarshal(b, &next); e != nil {
				return e
			}
			return scaler.UpdateConfig(next)
		}
		if e = restorePolicy(startup, store, "autoscaling", scaler.Config(), api.ApplyAutoscaling); e != nil {
			return e
		}
		launch(func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				iteration, cancel := context.WithTimeout(ctx, 15*time.Second)
				decision, e := scaler.Reconcile(iteration)
				cancel()
				if e != nil && ctx.Err() == nil {
					slog.Error("autoscaler reconciliation", "error", e)
				} else if len(decision.Created)+len(decision.Draining)+len(decision.Removed) > 0 {
					slog.Info("autoscaler decision", "created", decision.Created, "draining", decision.Draining, "removed", decision.Removed)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}
	go func() { errs <- public.Serve(pl) }()
	go func() { errs <- internal.Serve(il) }()
	go func() { errs <- httpServer.Serve(hl) }()
	slog.Info("FluxForge ready", "public", c.PublicAddr, "internal", c.InternalAddr, "http", c.HTTPAddr)
	select {
	case <-ctx.Done():
		e = nil
	case e = <-errs:
	}
	ready.Store(false)
	cancel()
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	httpServer.Shutdown(shutdown)
	stopped := make(chan struct{})
	go func() { public.GracefulStop(); internal.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutdown.Done():
		public.Stop()
		internal.Stop()
	}
	workers.Wait()
	return e
}
