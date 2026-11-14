package grpc

import (
	"context"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/workflow"
	pb "fluxforge-control/pkg/api/v1"
	"google.golang.org/grpc/metadata"
	"strings"
	"testing"
)

type captureSubmit struct {
	workflow.Repository
	command domain.SubmitCommand
}

func (r *captureSubmit) Submit(_ context.Context, c domain.SubmitCommand) (domain.Run, bool, error) {
	r.command = c
	return domain.Run{ID: "run", TraceParent: c.TraceParent}, false, nil
}
func TestSubmitContinuesW3CMetadataWithoutReusingCallerSpan(t *testing.T) {
	repo := &captureSubmit{}
	s := &Server{Workflow: workflow.New(repo, nil, nil)}
	caller := "00-0123456789abcdef0123456789abcdef-1122334455667788-01"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", caller))
	_, e := s.SubmitRun(ctx, &pb.SubmitRunRequest{WorkflowId: "workflow", WorkflowVersion: "v1", IdempotencyKey: "key", Input: &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: []byte(`{}`)}}})
	if e != nil {
		t.Fatal(e)
	}
	if repo.command.ParentSpanID != "1122334455667788" || repo.command.TraceParent == caller || !strings.HasPrefix(repo.command.TraceParent, "00-0123456789abcdef0123456789abcdef-") {
		t.Fatalf("caller/Run context not continued: %+v", repo.command)
	}
}
