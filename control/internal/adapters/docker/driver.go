// Package docker adapts the local Docker Engine API to autoscaler.Driver.
package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"fluxforge-control/internal/autoscaler"
)

const ManagedLabel = "io.fluxforge.managed"
const ProjectLabel = "io.fluxforge.project"
const RunnerLabel = "io.fluxforge.runner-id"

type Config struct {
	SocketPath  string
	Image       string
	Network     string
	Project     string
	Environment map[string]string
	MemoryBytes int64
	CPUMillis   int64
	PIDsLimit   int64
	CacheBytes  int64
	Timeout     time.Duration
	// HTTPClient/BaseURL support deterministic API tests. Production uses Unix socket.
	HTTPClient *http.Client
	BaseURL    string
}
type Driver struct {
	config     Config
	client     *http.Client
	baseURL    string
	mu         sync.Mutex
	apiVersion string
}
type engineContainer struct {
	ID      string            `json:"Id"`
	Created int64             `json:"Created"`
	State   string            `json:"State"`
	Labels  map[string]string `json:"Labels"`
}
type inspectedContainer struct {
	ID      string `json:"Id"`
	Created string `json:"Created"`
	Config  struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Status string `json:"Status"`
	} `json:"State"`
}
type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string { return fmt.Sprintf("Docker API HTTP %d: %s", e.Status, e.Message) }

func New(config Config) (*Driver, error) {
	if config.Image == "" || config.Network == "" {
		return nil, errors.New("Docker runner image and network are required")
	}
	if config.Project == "" {
		config.Project = "fluxforge"
	}
	if config.SocketPath == "" {
		config.SocketPath = "/var/run/docker.sock"
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Second
	}
	if config.MemoryBytes <= 0 {
		config.MemoryBytes = 1024 * 1024 * 1024
	}
	if config.CPUMillis <= 0 {
		config.CPUMillis = 2000
	}
	if config.PIDsLimit <= 0 {
		config.PIDsLimit = 128
	}
	if config.CacheBytes <= 0 {
		config.CacheBytes = 256 * 1024 * 1024
	}
	client := config.HTTPClient
	if client == nil {
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", config.SocketPath)
		}}
		client = &http.Client{Transport: transport, Timeout: config.Timeout}
	}
	base := config.BaseURL
	if base == "" {
		base = "http://docker"
	}
	return &Driver{config: config, client: client, baseURL: strings.TrimRight(base, "/")}, nil
}

func (d *Driver) request(ctx context.Context, method, path string, body any, result any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 8192))
		return &apiError{Status: response.StatusCode, Message: strings.TrimSpace(string(data))}
	}
	if result == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 1024*1024))
		return err
	}
	return json.NewDecoder(io.LimitReader(response.Body, 8*1024*1024)).Decode(result)
}

func (d *Driver) version(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.apiVersion != "" {
		return d.apiVersion, nil
	}
	var response struct {
		APIVersion string `json:"ApiVersion"`
	}
	if err := d.request(ctx, http.MethodGet, "/version", nil, &response); err != nil {
		return "", err
	}
	if response.APIVersion == "" || strings.ContainsAny(response.APIVersion, "/?#") {
		return "", errors.New("invalid Docker API version")
	}
	d.apiVersion = "/v" + response.APIVersion
	return d.apiVersion, nil
}
func (d *Driver) owned(labels map[string]string) bool {
	return labels[ManagedLabel] == "true" && labels[ProjectLabel] == d.config.Project && labels[RunnerLabel] != ""
}
func (d *Driver) inspect(ctx context.Context, prefix, id string) (inspectedContainer, error) {
	var result inspectedContainer
	err := d.request(ctx, http.MethodGet, prefix+"/containers/"+url.PathEscape(id)+"/json", nil, &result)
	return result, err
}

func (d *Driver) List(ctx context.Context) ([]autoscaler.ManagedRunner, error) {
	prefix, err := d.version(ctx)
	if err != nil {
		return nil, err
	}
	filters, _ := json.Marshal(map[string][]string{"label": {ManagedLabel + "=true", ProjectLabel + "=" + d.config.Project}})
	var containers []engineContainer
	if err = d.request(ctx, http.MethodGet, prefix+"/containers/json?all=1&filters="+url.QueryEscape(string(filters)), nil, &containers); err != nil {
		return nil, err
	}
	result := make([]autoscaler.ManagedRunner, 0, len(containers))
	for _, c := range containers {
		if d.owned(c.Labels) {
			result = append(result, autoscaler.ManagedRunner{ID: c.Labels[RunnerLabel], ContainerID: c.ID, State: c.State, CreatedAt: time.Unix(c.Created, 0)})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (d *Driver) Create(ctx context.Context, spec autoscaler.RunnerSpec) (autoscaler.ManagedRunner, error) {
	if spec.ID == "" || strings.ContainsAny(spec.ID, "/\\?# ") || spec.Slots <= 0 {
		return autoscaler.ManagedRunner{}, errors.New("invalid Runner spec")
	}
	prefix, err := d.version(ctx)
	if err != nil {
		return autoscaler.ManagedRunner{}, err
	}
	envMap := make(map[string]string, len(d.config.Environment)+2)
	for k, v := range d.config.Environment {
		envMap[k] = v
	}
	envMap["RUNNER_ID"] = spec.ID
	envMap["RUNNER_SLOTS"] = fmt.Sprint(spec.Slots)
	envMap["RUNNER_MEMORY_BYTES"] = fmt.Sprint(d.config.MemoryBytes)
	envMap["RUNNER_CPU_MILLIS"] = fmt.Sprint(d.config.CPUMillis)
	keys := make([]string, 0, len(envMap))
	for k := range envMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	environment := make([]string, 0, len(keys))
	for _, k := range keys {
		environment = append(environment, k+"="+envMap[k])
	}
	body := map[string]any{
		"Image": d.config.Image, "User": "65532:65532", "Env": environment, "Labels": map[string]string{ManagedLabel: "true", ProjectLabel: d.config.Project, RunnerLabel: spec.ID},
		"HostConfig": map[string]any{"NetworkMode": d.config.Network, "Memory": d.config.MemoryBytes, "MemorySwap": d.config.MemoryBytes, "NanoCpus": d.config.CPUMillis * 1000000, "PidsLimit": d.config.PIDsLimit, "Init": true, "CapDrop": []string{"ALL"}, "SecurityOpt": []string{"no-new-privileges:true"}, "ReadonlyRootfs": true, "Tmpfs": map[string]string{"/tmp": fmt.Sprintf("rw,exec,nosuid,nodev,mode=1777,size=%d", d.config.CacheBytes)}, "LogConfig": map[string]any{"Type": "json-file", "Config": map[string]string{"max-size": "10m", "max-file": "2"}}},
	}
	var response struct {
		ID string `json:"Id"`
	}
	err = d.request(ctx, http.MethodPost, prefix+"/containers/create?name="+url.QueryEscape(d.config.Project+"-"+spec.ID), body, &response)
	if err != nil {
		var apiErr *apiError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
			return autoscaler.ManagedRunner{}, err
		}
		existing, inspectErr := d.inspect(ctx, prefix, d.config.Project+"-"+spec.ID)
		if inspectErr != nil {
			return autoscaler.ManagedRunner{}, inspectErr
		}
		if !d.owned(existing.Config.Labels) || existing.Config.Labels[RunnerLabel] != spec.ID || existing.Config.Image != d.config.Image {
			return autoscaler.ManagedRunner{}, errors.New("container name conflict belongs to another workload")
		}
		response.ID = existing.ID
	}
	if response.ID == "" {
		return autoscaler.ManagedRunner{}, errors.New("Docker create did not return a container ID")
	}
	if err = d.request(ctx, http.MethodPost, prefix+"/containers/"+url.PathEscape(response.ID)+"/start", nil, nil); err != nil {
		var apiErr *apiError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusNotModified {
			return autoscaler.ManagedRunner{}, err
		}
	}
	return autoscaler.ManagedRunner{ID: spec.ID, ContainerID: response.ID, State: "running", CreatedAt: time.Now()}, nil
}

// Remove verifies both ownership labels again. The controller must first prove
// the runner drained; the adapter never deletes containers from another project.
func (d *Driver) Remove(ctx context.Context, id string) error {
	prefix, err := d.version(ctx)
	if err != nil {
		return err
	}
	container, err := d.inspect(ctx, prefix, id)
	if err != nil {
		var apiErr *apiError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
			return nil
		}
		return err
	}
	if !d.owned(container.Config.Labels) {
		return errors.New("refusing to remove a container without this project's managed labels")
	}
	// Ordinary removal first asks the empty agent/host to exit and then refuses
	// to delete a still-running container. A forced administrative kill is a
	// separate operation and never hidden inside scale-down.
	err = d.request(ctx, http.MethodPost, prefix+"/containers/"+url.PathEscape(container.ID)+"/stop?t=5", nil, nil)
	var stopErr *apiError
	if err != nil && (!errors.As(err, &stopErr) || (stopErr.Status != http.StatusNotModified && stopErr.Status != http.StatusNotFound)) {
		return err
	}
	err = d.request(ctx, http.MethodDelete, prefix+"/containers/"+url.PathEscape(container.ID)+"?force=0", nil, nil)
	var apiErr *apiError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
		return nil
	}
	return err
}
