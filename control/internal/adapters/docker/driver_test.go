package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fluxforge-control/internal/autoscaler"
)

func TestCreateNegotiatesVersionAndAppliesBoundaries(t *testing.T) {
	versionCalls := 0
	created := false
	started := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/version":
			versionCalls++
			_, _ = w.Write([]byte(`{"ApiVersion":"1.45"}`))
		case r.URL.Path == "/v1.45/containers/create":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["Image"] != "runner@sha256:fixed" {
				t.Error("image changed")
			}
			host := body["HostConfig"].(map[string]any)
			if host["NetworkMode"] != "fluxforge-net" || host["ReadonlyRootfs"] != true || host["PidsLimit"].(float64) != 128 {
				t.Errorf("missing confinement: %+v", host)
			}
			if body["User"] != "65532:65532" || host["Init"] != true || host["MemorySwap"] != host["Memory"] {
				t.Error("missing non-root, init or swap confinement")
			}
			tmp := host["Tmpfs"].(map[string]any)["/tmp"].(string)
			if strings.Contains(tmp, "noexec") || !strings.Contains(tmp, "mode=1777") {
				t.Error("shared-library cache must allow executable mappings and non-root writes")
			}
			labels := body["Labels"].(map[string]any)
			if labels[ManagedLabel] != "true" || labels[ProjectLabel] != "test" {
				t.Error("missing ownership labels")
			}
			env := body["Env"].([]any)
			foundID, foundSlots, foundMemory, foundCPU := false, false, false, false
			for _, item := range env {
				if item == "RUNNER_ID=r1" {
					foundID = true
				}
				if item == "RUNNER_SLOTS=2" {
					foundSlots = true
				}
				if item == "RUNNER_MEMORY_BYTES=1073741824" {
					foundMemory = true
				}
				if item == "RUNNER_CPU_MILLIS=2000" {
					foundCPU = true
				}
			}
			if !foundID || !foundSlots || !foundMemory || !foundCPU {
				t.Errorf("missing runner environment %+v", env)
			}
			created = true
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"Id":"container-1"}`))
		case r.URL.Path == "/v1.45/containers/container-1/start":
			started = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	d, err := New(Config{Image: "runner@sha256:fixed", Network: "fluxforge-net", Project: "test", HTTPClient: server.Client(), BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	result, err := d.Create(context.Background(), autoscaler.RunnerSpec{ID: "r1", Slots: 2})
	if err != nil || result.ContainerID != "container-1" || !created || !started || versionCalls != 1 {
		t.Fatal(result, err)
	}
}

func TestListFiltersAndRemoveRejectsForeignContainers(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/version":
			_, _ = w.Write([]byte(`{"ApiVersion":"1.45"}`))
		case r.URL.Path == "/v1.45/containers/json":
			if !strings.Contains(r.URL.Query().Get("filters"), ProjectLabel+"=test") {
				t.Error("unscoped container discovery")
			}
			_ = json.NewEncoder(w).Encode([]engineContainer{{ID: "ours", Created: 100, State: "running", Labels: map[string]string{ManagedLabel: "true", ProjectLabel: "test", RunnerLabel: "r1"}}, {ID: "foreign", Labels: map[string]string{ManagedLabel: "true", ProjectLabel: "another", RunnerLabel: "r2"}}})
		case strings.HasSuffix(r.URL.Path, "/foreign/json"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Id": "foreign", "Config": map[string]any{"Labels": map[string]string{ManagedLabel: "true", ProjectLabel: "another", RunnerLabel: "r2"}}})
		case r.Method == "DELETE":
			deleted = true
			w.WriteHeader(204)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	d, _ := New(Config{Image: "runner", Network: "net", Project: "test", HTTPClient: server.Client(), BaseURL: server.URL})
	list, err := d.List(context.Background())
	if err != nil || len(list) != 1 || list[0].ID != "r1" {
		t.Fatal(list, err)
	}
	if err = d.Remove(context.Background(), "foreign"); err == nil || deleted {
		t.Fatal("foreign container deletion allowed")
	}
}

func TestCreateNameConflictCannotAdoptUnrelatedContainer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/version":
			_, _ = w.Write([]byte(`{"ApiVersion":"1.45"}`))
		case strings.HasSuffix(r.URL.Path, "/create"):
			w.WriteHeader(409)
		case strings.HasSuffix(r.URL.Path, "/json"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Id": "other", "Config": map[string]any{"Image": "foreign", "Labels": map[string]string{}}})
		default:
			t.Error("attempted to start unrelated container")
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	d, _ := New(Config{Image: "runner", Network: "net", HTTPClient: server.Client(), BaseURL: server.URL})
	if _, err := d.Create(context.Background(), autoscaler.RunnerSpec{ID: "collision", Slots: 2}); err == nil {
		t.Fatal("adopted foreign name conflict")
	}
}

func TestRemoveOwnedContainerRechecksLabels(t *testing.T) {
	deleted := false
	stopped := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/version":
			_, _ = w.Write([]byte(`{"ApiVersion":"1.45"}`))
		case strings.HasSuffix(r.URL.Path, "/json"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Id": "ours", "Config": map[string]any{"Labels": map[string]string{ManagedLabel: "true", ProjectLabel: "test", RunnerLabel: "r"}}})
		case strings.HasSuffix(r.URL.Path, "/stop"):
			stopped = true
			if r.URL.Query().Get("t") != "5" {
				t.Error("missing bounded graceful stop")
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete:
			deleted = true
			if !stopped || r.URL.Query().Get("force") != "0" || r.URL.Query().Get("v") != "" {
				t.Error("scale-down forced removal, removed volumes, or skipped graceful stop")
			}
			w.WriteHeader(204)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	d, _ := New(Config{Image: "runner", Network: "net", Project: "test", HTTPClient: server.Client(), BaseURL: server.URL})
	if err := d.Remove(context.Background(), "ours"); err != nil || !deleted || !stopped {
		t.Fatal(deleted, err)
	}
}

func TestFailedStopDoesNotForceDelete(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/version":
			_, _ = w.Write([]byte(`{"ApiVersion":"1.45"}`))
		case strings.HasSuffix(r.URL.Path, "/json"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Id": "ours", "Config": map[string]any{"Labels": map[string]string{ManagedLabel: "true", ProjectLabel: "test", RunnerLabel: "r"}}})
		case strings.HasSuffix(r.URL.Path, "/stop"):
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodDelete:
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	d, _ := New(Config{Image: "runner", Network: "net", Project: "test", HTTPClient: server.Client(), BaseURL: server.URL})
	if err := d.Remove(context.Background(), "ours"); err == nil || deleted {
		t.Fatal("failed stop escalated to forced deletion")
	}
}
