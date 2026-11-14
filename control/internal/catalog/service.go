// Package catalog owns publication and validation of immutable execution specs.
package catalog

import (
	"context"
	"fluxforge-control/internal/domain"
	"fmt"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"io"
	"strings"
)

type Repository interface {
	PublishFunction(context.Context, domain.FunctionSpec) error
	GetFunction(context.Context, string, string) (domain.FunctionSpec, error)
	PublishWorkflow(context.Context, domain.WorkflowSpec) error
	GetWorkflow(context.Context, string, string) (domain.WorkflowSpec, error)
}
type ReferenceResolver interface {
	Resolve(context.Context, domain.ObjectRef) (domain.ObjectRef, error)
}
type Service struct {
	repo    Repository
	objects ReferenceResolver
}

func New(repo Repository, objects ReferenceResolver) *Service {
	return &Service{repo: repo, objects: objects}
}
func compileSchema(data []byte) error {
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft7
	c.LoadURL = func(url string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("external schema reference %s is disabled", url)
	}
	if e := c.AddResource("memory://schema.json", strings.NewReader(string(data))); e != nil {
		return e
	}
	_, e := c.Compile("memory://schema.json")
	return e
}
func (s *Service) PublishFunction(ctx context.Context, f domain.FunctionSpec) (domain.FunctionSpec, error) {
	if e := domain.NormalizeFunction(&f); e != nil {
		return f, e
	}
	if e := compileSchema(f.InputSchema); e != nil {
		return f, domain.E(domain.Invalid, "input schema: %v", e)
	}
	if e := compileSchema(f.OutputSchema); e != nil {
		return f, domain.E(domain.Invalid, "output schema: %v", e)
	}
	if s.objects != nil {
		ref, e := s.objects.Resolve(ctx, f.Artifact)
		if e != nil {
			return f, e
		}
		f.Artifact = ref
	}
	return f, s.repo.PublishFunction(ctx, f)
}
func (s *Service) GetFunction(ctx context.Context, id, version string) (domain.FunctionSpec, error) {
	return s.repo.GetFunction(ctx, id, version)
}
func (s *Service) PublishWorkflow(ctx context.Context, w domain.WorkflowSpec) (domain.WorkflowSpec, error) {
	if e := domain.NormalizeWorkflow(&w); e != nil {
		return w, e
	}
	return w, s.repo.PublishWorkflow(ctx, w)
}
func (s *Service) GetWorkflow(ctx context.Context, id, version string) (domain.WorkflowSpec, error) {
	return s.repo.GetWorkflow(ctx, id, version)
}
