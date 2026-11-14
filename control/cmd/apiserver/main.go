package main

import (
	"context"
	"fluxforge-control/internal/bootstrap"
	"fluxforge-control/internal/config"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if e := bootstrap.Run(ctx, config.Load()); e != nil {
		slog.Error("control plane stopped", "error", e)
		os.Exit(1)
	}
}
