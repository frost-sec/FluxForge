package domain

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func ValidatePayload(p PayloadRef) error {
	if (len(p.Inline) > 0) == (p.Object != nil) {
		return E(Invalid, "payload must contain exactly one of inline JSON or an object reference")
	}
	if p.Object != nil {
		return ValidateObject(*p.Object)
	}
	if len(p.Inline) > InlineLimit {
		return E(Invalid, "inline payload exceeds %d bytes; upload it to object storage", InlineLimit)
	}
	if !json.Valid(p.Inline) {
		return E(Invalid, "payload is not valid JSON")
	}
	return nil
}

func ValidateObject(o ObjectRef) error {
	if o.ID == "" && (o.Bucket == "" || o.Key == "") {
		return E(Invalid, "object reference requires an object ID")
	}
	if o.Size < 0 {
		return E(Invalid, "object size cannot be negative")
	}
	if len(o.SHA256) != 64 {
		return E(Invalid, "object SHA-256 must contain 64 hexadecimal characters")
	}
	if _, err := hex.DecodeString(o.SHA256); err != nil {
		return E(Invalid, "object SHA-256 is invalid")
	}
	return nil
}

func NormalizeFunction(f *FunctionSpec) error {
	if !namePattern.MatchString(f.ID) || !namePattern.MatchString(f.Version) {
		return E(Invalid, "invalid function ID or version")
	}
	if f.Runtime == "" {
		f.Runtime = "cpp20"
	}
	if f.Runtime != "cpp20" {
		return E(Invalid, "only cpp20 is supported")
	}
	if f.Architecture == "" {
		f.Architecture = "linux/amd64"
	}
	if f.Architecture != "linux/amd64" {
		return E(Invalid, "first release requires linux/amd64 artifacts")
	}
	if f.RuntimeDigest == "" {
		f.RuntimeDigest = "cpp20-linux-amd64-v1"
	}
	if err := ValidateObject(f.Artifact); err != nil {
		return err
	}
	if f.ABIVersion == 0 {
		f.ABIVersion = 1
	}
	if f.ABIVersion != 1 {
		return E(Invalid, "unsupported function ABI version")
	}
	if f.Entrypoint == "" {
		f.Entrypoint = "ff_invoke_v1"
	}
	if f.Entrypoint != "ff_invoke_v1" {
		return E(Invalid, "ABI v1 entrypoint is ff_invoke_v1")
	}
	if f.TimeoutMS == 0 {
		f.TimeoutMS = 5000
	}
	if f.TimeoutMS < 1 || f.TimeoutMS > 300000 {
		return E(Invalid, "function timeout must be 1..300000ms")
	}
	if f.MemoryBytes == 0 {
		f.MemoryBytes = 128 * 1024 * 1024
	}
	if f.MemoryBytes < 16*1024*1024 || f.MemoryBytes > 16*1024*1024*1024 {
		return E(Invalid, "function memory must be 16MiB..16GiB")
	}
	if f.CPUMillis == 0 {
		f.CPUMillis = 1000
	}
	if f.CPUMillis < 1 || f.CPUMillis > 64000 {
		return E(Invalid, "function CPU must be 1..64000 millicores")
	}
	if f.SLOMS == 0 {
		f.SLOMS = 1000
	}
	if f.SLOMS < 1 {
		return E(Invalid, "function SLO must be positive")
	}
	if len(f.InputSchema) == 0 {
		f.InputSchema = json.RawMessage(`{}`)
	}
	if len(f.OutputSchema) == 0 {
		f.OutputSchema = json.RawMessage(`{}`)
	}
	if !json.Valid(f.InputSchema) || !json.Valid(f.OutputSchema) {
		return E(Invalid, "function schemas must be JSON")
	}
	return nil
}

func NormalizeWorkflow(w *WorkflowSpec) error {
	if !namePattern.MatchString(w.ID) || !namePattern.MatchString(w.Version) {
		return E(Invalid, "invalid workflow ID or version")
	}
	if len(w.Steps) == 0 || len(w.Steps) > 1000 {
		return E(Invalid, "workflow must contain 1..1000 steps")
	}
	if w.DefaultSLOMS == 0 {
		w.DefaultSLOMS = 5000
	}
	if w.DefaultSLOMS < 1 {
		return E(Invalid, "workflow SLO must be positive")
	}
	byName := make(map[string]int, len(w.Steps))
	for i, s := range w.Steps {
		if !namePattern.MatchString(s.Name) {
			return E(Invalid, "invalid step name %q", s.Name)
		}
		if _, ok := byName[s.Name]; ok {
			return E(Invalid, "duplicate step %q", s.Name)
		}
		byName[s.Name] = i
	}
	for i := range w.Steps {
		s := &w.Steps[i]
		if !namePattern.MatchString(s.Function.ID) || !namePattern.MatchString(s.Function.Version) {
			return E(Invalid, "step %s requires an immutable function ID and version", s.Name)
		}
		if s.Input.Source == nil && len(s.Input.Fields) == 0 {
			s.Input.Source = &InputSource{Kind: "run_input"}
		}
		if s.Input.Source != nil && len(s.Input.Fields) > 0 {
			return E(Invalid, "step %s input cannot combine passthrough and fields", s.Name)
		}
		deps := map[string]bool{}
		for _, d := range s.DependsOn {
			deps[d] = true
		}
		validateSource := func(src InputSource) error {
			if err := ValidatePointer(src.Pointer); err != nil {
				return err
			}
			switch src.Kind {
			case "run_input":
			case "step_output":
				deps[src.Step] = true
			case "literal":
				if !json.Valid(src.Literal) || len(src.Literal) > InlineLimit {
					return E(Invalid, "step %s has an invalid or oversized literal", s.Name)
				}
			default:
				return E(Invalid, "step %s has unknown input source %q", s.Name, src.Kind)
			}
			return nil
		}
		if s.Input.Source != nil {
			if err := validateSource(*s.Input.Source); err != nil {
				return err
			}
		}
		for field, src := range s.Input.Fields {
			if field == "" {
				return E(Invalid, "input field names must not be empty")
			}
			if err := validateSource(src); err != nil {
				return err
			}
		}
		s.DependsOn = nil
		for d := range deps {
			if _, ok := byName[d]; !ok {
				return E(Invalid, "step %s refers to missing step %s", s.Name, d)
			}
			if d == s.Name {
				return E(Invalid, "step cannot depend on itself")
			}
			s.DependsOn = append(s.DependsOn, d)
		}
		sort.Strings(s.DependsOn)
		if s.Retry.MaxAttempts == 0 {
			s.Retry.MaxAttempts = 3
		}
		if s.Retry.MaxAttempts < 1 || s.Retry.MaxAttempts > 10 {
			return E(Invalid, "max_attempts must be 1..10")
		}
		if s.Retry.InitialBackoffMS == 0 {
			s.Retry.InitialBackoffMS = 100
		}
		if s.Retry.MaxBackoffMS == 0 {
			s.Retry.MaxBackoffMS = 5000
		}
		if s.Retry.InitialBackoffMS < 1 || s.Retry.MaxBackoffMS < s.Retry.InitialBackoffMS || s.Retry.MaxBackoffMS > 60000 {
			return E(Invalid, "invalid retry backoff")
		}
	}
	indegree := map[string]int{}
	children := map[string][]string{}
	for _, s := range w.Steps {
		indegree[s.Name] = len(s.DependsOn)
		for _, d := range s.DependsOn {
			children[d] = append(children[d], s.Name)
		}
	}
	queue := []string{}
	for n, d := range indegree {
		if d == 0 {
			queue = append(queue, n)
		}
	}
	visited := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		visited++
		for _, c := range children[n] {
			indegree[c]--
			if indegree[c] == 0 {
				queue = append(queue, c)
			}
		}
	}
	if visited != len(w.Steps) {
		return E(Invalid, "workflow contains a dependency cycle")
	}
	if w.OutputStep == "" {
		for _, s := range w.Steps {
			if len(children[s.Name]) == 0 {
				if w.OutputStep != "" {
					return E(Invalid, "multiple sink steps require output_step")
				}
				w.OutputStep = s.Name
			}
		}
	}
	if _, ok := byName[w.OutputStep]; !ok {
		return E(Invalid, "output_step does not exist")
	}
	if len(children[w.OutputStep]) > 0 {
		return E(Invalid, "output_step must be a sink")
	}
	return nil
}

func ValidatePointer(p string) error {
	if p == "" {
		return nil
	}
	if !strings.HasPrefix(p, "/") || len(p) > 1024 {
		return E(Invalid, "invalid JSON pointer")
	}
	for i := 0; i < len(p); i++ {
		if p[i] == '~' {
			if i+1 >= len(p) || (p[i+1] != '0' && p[i+1] != '1') {
				return E(Invalid, "invalid JSON pointer escape")
			}
			i++
		}
	}
	return nil
}

func ResolveInput(plan InputPlan, runInput PayloadRef, outputs map[string]PayloadRef) (InputPlan, error) {
	resolve := func(s InputSource) (InputSource, error) {
		var p PayloadRef
		switch s.Kind {
		case "run_input":
			p = runInput
		case "step_output":
			var ok bool
			p, ok = outputs[s.Step]
			if !ok {
				return s, fmt.Errorf("missing output for step %s", s.Step)
			}
		case "literal":
			p = PayloadRef{Inline: s.Literal}
		default:
			return s, E(Invalid, "invalid source kind")
		}
		return InputSource{Kind: "payload", Pointer: s.Pointer, Payload: &p}, nil
	}
	result := InputPlan{}
	if plan.Source != nil {
		s, e := resolve(*plan.Source)
		if e != nil {
			return result, e
		}
		result.Source = &s
	} else {
		result.Fields = map[string]InputSource{}
		for k, v := range plan.Fields {
			s, e := resolve(v)
			if e != nil {
				return result, e
			}
			result.Fields[k] = s
		}
	}
	return result, nil
}
