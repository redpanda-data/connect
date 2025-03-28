/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package mcp

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"

	"github.com/mark3labs/mcp-go/server"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/mcp/repository"
	"github.com/redpanda-data/connect/v4/internal/mcp/starlark"
	"github.com/redpanda-data/connect/v4/internal/mcp/tools"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// Run an mcp server against a target directory, with an optional base URL for
// an HTTP server.
func Run(logger *slog.Logger, repositoryDir, baseURLStr string) error {
	// Create MCP server
	s := server.NewMCPServer(
		"Redpanda Runtime",
		"1.0.0",
	)

	resWrapper := tools.NewResourcesWrapper(logger, s)

	repoScanner := repository.NewScanner(os.DirFS(repositoryDir))
	repoScanner.OnResourceFile(func(resourceType string, filename string, contents []byte) error {
		if filepath.Ext(filename) == ".star" {
			result, err := starlark.Eval(
				service.GlobalEnvironment(),
				nil,
				filename,
				contents,
			)
			if err != nil {
				return err
			}
			for k, v := range result.Processors {
				cfg := map[string]any{
					"label": k,
					v.Name:  v.SerializedConfig,
					"meta": map[string]any{
						"mcp": map[string]any{
							"enabled": true,
						},
					},
				}
				b, err := json.Marshal(&cfg)
				if err != nil {
					return err
				}
				if err := resWrapper.AddProcessor(b); err != nil {
					return err
				}
			}
			return nil
		}
		switch resourceType {
		case "cache":
			if err := resWrapper.AddCache(contents); err != nil {
				return err
			}
		case "processor":
			if err := resWrapper.AddProcessor(contents); err != nil {
				return err
			}
		default:
			return fmt.Errorf("resource type '%v' is not supported yet", resourceType)
		}
		return nil
	})
	if err := repoScanner.Scan("."); err != nil {
		return err
	}

	if err := resWrapper.Build(); err != nil {
		return err
	}

	if baseURLStr != "" {
		baseURL, err := url.Parse(baseURLStr)
		if err != nil {
			return err
		}

		sseServer := server.NewSSEServer(s, server.WithBaseURL(baseURLStr))
		logger.Info("SSE server listening")
		if err := sseServer.Start(":" + baseURL.Port()); err != nil {
			return err
		}
	} else {
		if err := server.ServeStdio(s); err != nil {
			return err
		}
	}

	return nil
}
