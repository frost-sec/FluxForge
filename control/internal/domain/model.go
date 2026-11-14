// Package domain contains the platform's transport-independent contracts.
package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const InlineLimit = 64 * 1024

type ObjectRef struct {
	ID          string `json:"id"`
	Bucket      string `json:"bucket,omitempty"`
	Key         string `json:"key,omitempty"`
	VersionID   string `json:"version_id,omitempty"`
	SHA256      string `json:"sha256"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type,omitempty"`
}

type PayloadRef struct {
	Inline json.RawMessage `json:"inline,omitempty"`
	Object *ObjectRef      `json:"object,omitempty"`
}

type FunctionRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type FunctionSpec struct {
	ID               string          `json:"id"`
	Version          string          `json:"version"`
	Runtime          string          `json:"runtime"`
	Architecture     string          `json:"architecture"`
	RuntimeDigest    string          `json:"runtime_digest"`
	Artifact         ObjectRef       `json:"artifact"`
	DependencyDigest string          `json:"dependency_digest"`
	ABIVersion       uint32          `json:"abi_version"`
	Entrypoint       string          `json:"entrypoint"`
	InputSchema      json.RawMessage `json:"input_schema"`
	OutputSchema     json.RawMessage `json:"output_schema"`
	TimeoutMS        int64           `json:"timeout_ms"`
	MemoryBytes      int64           `json:"memory_bytes"`
	CPUMillis        int64           `json:"cpu_millis"`
	SLOMS            int64           `json:"slo_ms"`
}

func (f FunctionSpec) AffinityKey() string {
	b, _ := json.Marshal([]any{f.ID, f.Version, f.Runtime, f.Architecture, f.RuntimeDigest, f.DependencyDigest, f.Artifact.SHA256, f.ABIVersion})
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type InputSource struct {
	Kind    string          `json:"kind"` // run_input, step_output, literal, payload (resolved)
	Step    string          `json:"step,omitempty"`
	Pointer string          `json:"pointer,omitempty"`
	Literal json.RawMessage `json:"literal,omitempty"`
	Payload *PayloadRef     `json:"payload,omitempty"`
}

type InputPlan struct {
	Source *InputSource           `json:"source,omitempty"`
	Fields map[string]InputSource `json:"fields,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts      int32 `json:"max_attempts"`
	InitialBackoffMS int64 `json:"initial_backoff_ms"`
	MaxBackoffMS     int64 `json:"max_backoff_ms"`
	RetryUserErrors  bool  `json:"retry_user_errors"`
}

type StepSpec struct {
	Name      string      `json:"name"`
	Function  FunctionRef `json:"function"`
	DependsOn []string    `json:"depends_on"`
	Input     InputPlan   `json:"input"`
	Retry     RetryPolicy `json:"retry"`
}

type WorkflowSpec struct {
	ID           string     `json:"id"`
	Version      string     `json:"version"`
	Steps        []StepSpec `json:"steps"`
	OutputStep   string     `json:"output_step"`
	DefaultSLOMS int64      `json:"default_slo_ms"`
}

type Run struct {
	ID              string      `json:"id"`
	WorkflowID      string      `json:"workflow_id"`
	WorkflowVersion string      `json:"workflow_version"`
	State           string      `json:"state"`
	Input           PayloadRef  `json:"input"`
	Output          *PayloadRef `json:"output,omitempty"`
	Priority        int32       `json:"priority"`
	SLOMS           int64       `json:"slo_ms"`
	CreatedAt       time.Time   `json:"created_at"`
	Deadline        time.Time   `json:"deadline"`
	FinishedAt      *time.Time  `json:"finished_at,omitempty"`
	Error           string      `json:"error,omitempty"`
	TraceParent     string      `json:"trace_parent,omitempty"`
	TraceState      string      `json:"trace_state,omitempty"`
	ParentSpanID    string      `json:"parent_span_id,omitempty"`
}

type Invocation struct {
	ID               string      `json:"id"`
	RunID            string      `json:"run_id"`
	StepName         string      `json:"step_name"`
	Function         FunctionRef `json:"function"`
	State            string      `json:"state"`
	RemainingDeps    int32       `json:"remaining_deps"`
	Generation       int64       `json:"generation"`
	AttemptNo        int32       `json:"attempt_no"`
	CurrentAttemptID string      `json:"current_attempt_id,omitempty"`
	FirstReadyAt     *time.Time  `json:"first_ready_at,omitempty"`
	ReadyAt          *time.Time  `json:"ready_at,omitempty"`
	NotBefore        *time.Time  `json:"not_before,omitempty"`
	StartedAt        *time.Time  `json:"started_at,omitempty"`
	FinishedAt       *time.Time  `json:"finished_at,omitempty"`
	Output           *PayloadRef `json:"output,omitempty"`
	Error            string      `json:"error,omitempty"`
}

type RunnerSession struct {
	ID                 string    `json:"id"`
	RunnerID           string    `json:"runner_id"`
	BootID             string    `json:"boot_id"`
	Policy             string    `json:"policy"`
	State              string    `json:"state"`
	Slots              int32     `json:"slots"`
	MemoryBytes        int64     `json:"memory_bytes"`
	CPUMillis          int64     `json:"cpu_millis"`
	AvailableSlots     int32     `json:"available_slots"`
	UsedMemoryBytes    int64     `json:"used_memory_bytes"`
	CachedKeys         []string  `json:"cached_keys"`
	LastSeen           time.Time `json:"last_seen"`
	ContainerID        string    `json:"container_id"`
	ObservedPolicy     string    `json:"observed_policy,omitempty"`
	ActiveAttempts     int32     `json:"active_attempts"`
	PendingCompletions int32     `json:"pending_completions"`
	DrainAcknowledged  bool      `json:"drain_acknowledged"`
	Runtimes           []string  `json:"runtimes"`
	Architecture       string    `json:"architecture"`
	RuntimeDigest      string    `json:"runtime_digest"`
}

type Attempt struct {
	ID                  string      `json:"id"`
	InvocationID        string      `json:"invocation_id"`
	RunID               string      `json:"run_id"`
	Number              int32       `json:"number"`
	SessionID           string      `json:"session_id"`
	State               string      `json:"state"`
	LeaseToken          string      `json:"-"`
	LeaseExpiresAt      time.Time   `json:"lease_expires_at"`
	ExecutionDeadline   *time.Time  `json:"execution_deadline,omitempty"`
	UploadDeadline      *time.Time  `json:"upload_deadline,omitempty"`
	ExecutionFinishedAt *time.Time  `json:"execution_finished_at,omitempty"`
	CreatedAt           time.Time   `json:"created_at"`
	StartedAt           *time.Time  `json:"started_at,omitempty"`
	FinishedAt          *time.Time  `json:"finished_at,omitempty"`
	DurationMS          int64       `json:"duration_ms"`
	Error               string      `json:"error,omitempty"`
	Output              *PayloadRef `json:"output,omitempty"`
}

type Assignment struct {
	Attempt    Attempt      `json:"attempt"`
	Invocation Invocation   `json:"invocation"`
	Run        Run          `json:"run"`
	Function   FunctionSpec `json:"function"`
	Input      InputPlan    `json:"input"`
}

type ReadyTask struct {
	InvocationID      string
	RunID             string
	Generation        int64
	FunctionKey       string
	FunctionID        string
	FunctionVersion   string
	ReadyAt           time.Time
	Priority          int32
	Deadline          time.Time
	EstimatedDuration time.Duration
	MemoryBytes       int64
	CPUMillis         int64
	Runtime           string
	RuntimeDigest     string
	SLODeadline       time.Time
}

type Completion struct {
	SessionID  string
	AttemptID  string
	LeaseToken string
	Outcome    string
	Output     *PayloadRef
	DurationMS int64
	Error      string
	Stages     []StageTiming
}

type StageTiming struct {
	Name       string `json:"name"`
	OffsetMS   int64  `json:"offset_ms"`
	DurationMS int64  `json:"duration_ms"`
}

type Event struct {
	ID           int64
	Kind         string
	InvocationID string
	RunID        string
	Generation   int64
	CreatedAt    time.Time
}

type SubmitCommand struct {
	WorkflowID, Version, IdempotencyKey string
	Input                               PayloadRef
	Priority                            int32
	SLOMS                               int64
	Deadline                            time.Time
	TraceParent, TraceState             string
	ParentSpanID                        string
}
type ClaimCommand struct {
	SessionID, RequestID string
	RequestedSlots       int32
	WaitTimeoutMS        int64
	Candidates           []ReadyTask
}
type ClaimResult struct {
	Assignments       []Assignment
	Duplicate         bool
	ExpiredAttemptIDs []string
	Policy            string
}
type CompletionResult struct {
	Duplicate          bool
	RunID              string
	Invocation         Invocation
	Function           FunctionSpec
	InvocationTerminal bool
}
type Capacity struct {
	Slots                  int32
	MemoryBytes, CPUMillis int64
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" + hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" + hex.EncodeToString(b[10:16])
}

func Token() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func Hash(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func ActiveAttempt(s string) bool { return s == "LEASED" || s == "RUNNING" || s == "COMPLETING" }
func TerminalInvocation(s string) bool {
	return s == "SUCCEEDED" || s == "FAILED" || s == "CANCELLED" || s == "TIMED_OUT"
}
func ActiveRun(s string) bool { return s == "PENDING" || s == "RUNNING" }
func TerminalRun(s string) bool {
	return s == "SUCCEEDED" || s == "FAILED" || s == "CANCELLED" || s == "TIMED_OUT"
}
