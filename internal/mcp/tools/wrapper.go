/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package tools

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"
)

// ResourcesWrapper attempts to parse resource files, adds those resources to
// a ResourcesBuilder as well as, where appropriate, adding them to an MCP
// server as tools.
type ResourcesWrapper struct {
	svr       *server.MCPServer
	builder   *service.ResourceBuilder
	resources *service.Resources
	closeFn   func(context.Context) error
}

// NewResourcesWrapper creates a new resources wrapper.
func NewResourcesWrapper(logger *slog.Logger, svr *server.MCPServer) *ResourcesWrapper {
	w := &ResourcesWrapper{
		svr:     svr,
		builder: service.NewResourceBuilder(),
	}
	w.builder.SetLogger(logger)
	// TODO: Add metrics
	return w
}

// Build the underlying ResourcesBuilder, which allows the resources to be
// executed.
func (w *ResourcesWrapper) Build() (err error) {
	w.resources, w.closeFn, err = w.builder.Build()
	return
}

// Close all underlying resources and their connections.
func (w *ResourcesWrapper) Close(ctx context.Context) error {
	closeFn := w.closeFn
	if closeFn == nil {
		return nil
	}
	w.resources = nil
	w.closeFn = nil
	return closeFn(ctx)
}

type mcpConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Description string `yaml:"description"`
}

type meta struct {
	MCP mcpConfig `yaml:"mcp"`
}

type resFile struct {
	Label string `yaml:"label"`
	Meta  meta   `yaml:"meta"`
}

// AddCache attempts to parse a cache resource config and adds it as an MCP tool
// if appropriate.
func (w *ResourcesWrapper) AddCache(fileBytes []byte) error {
	var res resFile
	if err := yaml.Unmarshal(fileBytes, &res); err != nil {
		return err
	}

	if err := w.builder.AddCacheYAML(string(fileBytes)); err != nil {
		return err
	}

	if !res.Meta.MCP.Enabled {
		return nil
	}

	w.svr.AddTool(mcp.NewTool("get-"+res.Label,
		mcp.WithDescription("Obtain an item from "+res.Meta.MCP.Description),
		mcp.WithString("key",
			mcp.Description("The key of the item to obtain."),
			mcp.Required(),
		),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		key, exists := request.Params.Arguments["key"].(string)
		if !exists {
			return nil, errors.New("missing key [string] argument")
		}

		var value []byte
		var getErr error
		if err := w.resources.AccessCache(ctx, res.Label, func(c service.Cache) {
			value, getErr = c.Get(ctx, key)
		}); err != nil {
			return nil, err
		}
		if getErr != nil {
			return nil, getErr
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				mcp.TextContent{
					Type: "text",
					Text: string(value),
				},
			},
		}, nil
	})

	w.svr.AddTool(mcp.NewTool("set-"+res.Label,
		mcp.WithDescription("Set an item within "+res.Meta.MCP.Description),
		mcp.WithString("key",
			mcp.Description("The key of the item to set."),
			mcp.Required(),
		),
		mcp.WithString("value",
			mcp.Description("The value of the item to set."),
			mcp.Required(),
		),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		key, exists := request.Params.Arguments["key"].(string)
		if !exists {
			return nil, errors.New("missing key [string] argument")
		}

		value, exists := request.Params.Arguments["value"].(string)
		if !exists {
			return nil, errors.New("missing value [string] argument")
		}

		var setErr error
		if err := w.resources.AccessCache(ctx, res.Label, func(c service.Cache) {
			setErr = c.Set(ctx, key, []byte(value), nil)
		}); err != nil {
			return nil, err
		}
		if setErr != nil {
			return nil, setErr
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				mcp.TextContent{
					Type: "text",
					Text: "Value set successfully",
				},
			},
		}, nil
	})

	return nil
}

// AddProcessor attempts to parse a processor resource config and adds it as an
// MCP tool if appropriate.
func (w *ResourcesWrapper) AddProcessor(fileBytes []byte) error {
	var res resFile
	if err := yaml.Unmarshal(fileBytes, &res); err != nil {
		return err
	}

	if err := w.builder.AddProcessorYAML(string(fileBytes)); err != nil {
		return err
	}

	if !res.Meta.MCP.Enabled {
		return nil
	}

	w.svr.AddTool(mcp.NewTool(res.Label,
		mcp.WithDescription(res.Meta.MCP.Description),
		mcp.WithString("value",
			mcp.Description("The value to execute the tool upon."),
			mcp.Required(),
		),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		value, exists := request.Params.Arguments["value"].(string)
		if !exists {
			return nil, errors.New("missing value [string] argument")
		}

		inMsg := service.NewMessage([]byte(value))

		var resBatch service.MessageBatch
		var procErr error
		if err := w.resources.AccessProcessor(ctx, res.Label, func(p *service.ResourceProcessor) {
			resBatch, procErr = p.Process(ctx, inMsg)
		}); err != nil {
			return nil, err
		}
		if procErr != nil {
			return nil, procErr
		}

		var content []mcp.Content
		for _, m := range resBatch {
			if err := m.GetError(); err != nil {
				return nil, err
			}

			mBytes, err := m.AsBytes()
			if err != nil {
				return nil, err
			}

			content = append(content, mcp.TextContent{
				Type: "text",
				Text: string(mBytes),
			})
		}

		return &mcp.CallToolResult{
			Content: content,
		}, nil
	})

	return nil
}
