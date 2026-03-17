// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mcp

import (
	"context"
	"log/slog"
	"net"

	"github.com/redpanda-data/connect/v4/internal/license"
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// Run an mcp server against a target directory, with an optional base URL for
// an HTTP server.
func Run(
	logger *slog.Logger,
	envVarLookupFunc func(context.Context, string) (string, bool),
	repositoryDir, addr, observabilityAddr string,
	tagFilterFunc func([]string) bool,
	license license.Config,
	auth *Authorizer,
) error {
	srv, err := NewServer(repositoryDir, logger, envVarLookupFunc, nil, tagFilterFunc, license, auth)
	if err != nil {
		return err
	}
	if addr == "" {
		return srv.ServeStdio()
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	// Start observability server on configured address (default :6060)
	if observabilityAddr != "" {
		obsListener, err := net.Listen("tcp", observabilityAddr)
		if err != nil {
			logger.Warn("Failed to start observability server", "error", err, "address", observabilityAddr)
		} else {
			logger.Info("Starting observability server", "address", observabilityAddr)
			go func() {
				if err := srv.ServeObservability(context.Background(), obsListener); err != nil {
					logger.Error("Observability server error", "error", err)
				}
			}()
			defer obsListener.Close()
		}
	}

	return srv.ServeHTTP(context.Background(), l)
}
