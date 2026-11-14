// Package config reads deployment configuration without starting dependencies.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DSN, MinIOEndpoint, MinIOPublicEndpoint, MinIOAccessKey, MinIOSecretKey, MinIOBucket string
	PublicAddr, InternalAddr, HTTPAddr, PublicToken, InternalToken                       string
	Project, RunnerImage, Network, RuntimeDigest, OTLPEndpoint                           string
	Autoscaling                                                                          bool
	MinRunners, MaxRunners, SlotsPerRunner                                               int
	AffinityDelay                                                                        time.Duration
}

func env(k, d string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return d
}
func number(k string, d int) int {
	if s := os.Getenv(k); s != "" {
		if n, e := strconv.Atoi(s); e == nil {
			return n
		}
	}
	return d
}
func Load() Config {
	return Config{DSN: env("DB_DSN", "postgres://fluxforge:password123@postgres:5432/fluxforge?sslmode=disable"), MinIOEndpoint: env("MINIO_ENDPOINT", "http://minio:9000"), MinIOPublicEndpoint: env("MINIO_PUBLIC_ENDPOINT", "http://localhost:19000"), MinIOAccessKey: env("MINIO_ACCESS_KEY", "admin"), MinIOSecretKey: env("MINIO_SECRET_KEY", "password123"), MinIOBucket: env("MINIO_BUCKET", "fluxforge"), PublicAddr: env("PUBLIC_ADDR", ":9090"), InternalAddr: env("INTERNAL_ADDR", ":9091"), HTTPAddr: env("HTTP_ADDR", ":8080"), PublicToken: env("FF_PUBLIC_TOKEN", ""), InternalToken: env("FF_INTERNAL_TOKEN", ""), Project: env("FF_PROJECT", "fluxforge-v1"), RunnerImage: env("RUNNER_IMAGE", "fluxforge-runner-runtime:dev"), Network: env("NETWORK_NAME", "fluxforge-v1_net"), RuntimeDigest: env("FF_RUNTIME_DIGEST", "cpp20-linux-amd64-v1"), OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", ""), Autoscaling: env("AUTOSCALING_ENABLED", "true") == "true", MinRunners: number("MIN_RUNNERS", 1), MaxRunners: number("MAX_RUNNERS", 4), SlotsPerRunner: number("RUNNER_SLOTS", 2), AffinityDelay: time.Duration(number("AFFINITY_DELAY_MS", 20)) * time.Millisecond}
}
