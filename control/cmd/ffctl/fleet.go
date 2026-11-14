package main

import (
	"encoding/json"
	pb "fluxforge-control/pkg/api/v1"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

func (c *client) fleet(action string, budget time.Duration) error {
	api := pb.NewQueueAdminServiceClient(c.internal)
	applied := false
	for i := 0; i < 3; i++ {
		ctx, cancel := c.ctx(true)
		old, e := api.GetAutoscalingPolicy(ctx, &pb.GetPolicyRequest{})
		cancel()
		if e != nil {
			return e
		}
		var config map[string]any
		if e = json.Unmarshal(old.Json, &config); e != nil {
			return e
		}
		if config == nil {
			config = map[string]any{}
		}
		config["Paused"] = action != "resume"
		b, e := json.Marshal(config)
		if e != nil {
			return e
		}
		ctx, cancel = c.ctx(true)
		_, e = api.SetAutoscalingPolicy(ctx, &pb.SetPolicyRequest{ExpectedVersion: old.Version, Json: b})
		cancel()
		if e == nil {
			applied = true
			break
		}
		if status.Code(e) != codes.AlreadyExists {
			return e
		}
	}
	if !applied {
		return fmt.Errorf("autoscaling policy changed repeatedly")
	}
	if action != "shutdown" {
		fmt.Println("fleet:", action)
		return nil
	}
	until := time.Now().Add(budget)
	for time.Now().Before(until) {
		ctx, cancel := c.ctx(true)
		runners, e := api.ListRunners(ctx, &pb.ListRunnersRequest{})
		cancel()
		if e != nil {
			return e
		}
		drained := true
		for _, r := range runners.Runners {
			if r.OccupiedSlots > 0 {
				drained = false
			}
			if r.State != "ACTIVE" {
				continue
			}
			if r.Policy != pb.RunnerStatePolicy_RUNNER_POLICY_DRAINING {
				ctx, cancel = c.ctx(true)
				_, e = api.SetRunnerPolicy(ctx, &pb.SetRunnerPolicyRequest{RunnerId: r.RunnerId, Policy: pb.RunnerStatePolicy_RUNNER_POLICY_DRAINING})
				cancel()
				if e != nil {
					return e
				}
				drained = false
				continue
			}
			if !r.DrainAcknowledged || r.ActiveAttempts > 0 || r.PendingCompletions > 0 {
				drained = false
			}
		}
		if drained {
			fmt.Println("fleet: drained; no active attempts or pending completions")
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("drain timeout: fleet remains paused; existing tasks continue, rerun shutdown after completion")
}
