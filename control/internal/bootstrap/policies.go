package bootstrap

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/adapters/postgres"
)

// Load persisted policy before serving traffic. Defaults are versioned once so
// GetPolicy returns the actual active configuration, not an unexplained {}.
func restorePolicy(ctx context.Context, store *postgres.Store, name string, defaults any, apply func([]byte) error) error {
	version, b, e := store.GetPolicy(ctx, name)
	if e != nil {
		return e
	}
	if version == 0 {
		b, e = json.Marshal(defaults)
		if e != nil {
			return e
		}
		if _, e = store.SetConfigPolicy(ctx, name, 0, b); e != nil {
			return e
		}
	}
	return apply(b)
}
