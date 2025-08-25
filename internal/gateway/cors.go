// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package gateway

import (
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/handlers"
)

const (
	// RPEnvCorsOrigins is the environment variable name for CORS allowed origins configuration.
	RPEnvCorsOrigins = "REDPANDA_CLOUD_GATEWAY_CORS_ORIGINS"
)

// CORSConfig holds CORS configuration settings.
type CORSConfig struct {
	enabled        bool
	allowedOrigins []string
}

// NewCORSConfigFromEnv creates a CORS configuration from environment variables.
func NewCORSConfigFromEnv() CORSConfig {
	var config CORSConfig
	if v := os.Getenv(RPEnvCorsOrigins); v != "" {
		config.enabled = true
		config.allowedOrigins = strings.Split(v, ",")
		for i, o := range config.allowedOrigins {
			config.allowedOrigins[i] = strings.TrimSpace(o)
		}
	}
	return config
}

// WrapHandler wraps an HTTP handler with CORS middleware if CORS is enabled.
func (conf CORSConfig) WrapHandler(handler http.Handler) http.Handler {
	if !conf.enabled {
		return handler
	}
	return handlers.CORS(
		handlers.AllowedOrigins(conf.allowedOrigins),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}),
	)(handler)
}
