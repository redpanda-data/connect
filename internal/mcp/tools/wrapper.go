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

package tools

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/redpanda-data/benthos/v4/public/service"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
)

// ResourcesWrapper attempts to parse resource files, adds those resources to
// a ResourcesBuilder as well as, where appropriate, adding them to an MCP
// server as tools.
type ResourcesWrapper struct {
	logger    *slog.Logger
	svr       *server.MCPServer
	builder   *service.ResourceBuilder
	resources *service.Resources
	closeFn   func(context.Context) error
	// TODO: Remove labels in favour of tags
	labelFilter func(label string) bool
	tagsFilter  func(tags []string) bool
}

// NewResourcesWrapper creates a new resources wrapper.
func NewResourcesWrapper(logger *slog.Logger, svr *server.MCPServer, labelFilter func(label string) bool, tagsFilter func(tags []string) bool) *ResourcesWrapper {
	if labelFilter == nil {
		labelFilter = func(label string) bool {
			return true
		}
	}
	if tagsFilter == nil {
		tagsFilter = func(tags []string) bool {
			return true
		}
	}
	w := &ResourcesWrapper{
		logger:      logger,
		svr:         svr,
		builder:     service.NewResourceBuilder(),
		labelFilter: labelFilter,
		tagsFilter:  tagsFilter,
	}
	w.builder.SetLogger(logger)
	return w
}

// SetEnvVarLookupFunc changes the behaviour of the resources wrapper so that
// the value of environment variable interpolations (of the form `${FOO}`) are
// obtained via a provided function rather than the default of os.LookupEnv.
func (w *ResourcesWrapper) SetEnvVarLookupFunc(fn func(context.Context, string) (string, bool)) {
	w.builder.SetEnvVarLookupFunc(fn)
}

// SetHTTPMultiplexer assigns a given HTTP multiplexer to be used by resources
// and metrics solutions to expose themselves as HTTP endpoints.
func (w *ResourcesWrapper) SetHTTPMultiplexer(mux service.HTTPMultiplexer) {
	w.builder.SetHTTPMux(mux)
}

// Build the underlying ResourcesBuilder, which allows the resources to be
// executed.
func (w *ResourcesWrapper) Build() (resources *service.Resources, err error) {
	resources, w.closeFn, err = w.builder.Build()
	w.resources = resources
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

func (w *ResourcesWrapper) initSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return w.resources.OtelTracer().Tracer("rpcn-mcp").Start(ctx, name)
}

func (w *ResourcesWrapper) initMsgSpan(name string, msg *service.Message) (*service.Message, trace.Span) {
	ctx, t := w.initSpan(msg.Context(), name)
	return msg.WithContext(ctx), t
}

type mcpProperty struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Description string `yaml:"description"`
	Required    bool   `yaml:"required"`
}

func (p mcpProperty) rawOption() (any, error) {
	return map[string]any{
		"name":        p.Name,
		"type":        p.Type,
		"description": p.Description,
		"required":    p.Required,
	}, nil
}

func (p mcpProperty) toolOption() (mcp.ToolOption, error) {
	var opts []mcp.PropertyOption
	if p.Required {
		opts = append(opts, mcp.Required())
	}
	if p.Description != "" {
		opts = append(opts, mcp.Description(p.Description))
	}

	switch p.Type {
	case "string":
		return mcp.WithString(p.Name, opts...), nil
	case "bool", "boolean":
		return mcp.WithBoolean(p.Name, opts...), nil
	case "number":
		return mcp.WithNumber(p.Name, opts...), nil
	}
	return nil, fmt.Errorf("property type '%v' not supported", p.Type)
}

type mcpConfig struct {
	Enabled     bool          `yaml:"enabled"`
	Description string        `yaml:"description"`
	Properties  []mcpProperty `yaml:"properties"`
}

type meta struct {
	Tags []string  `yaml:"tags"`
	MCP  mcpConfig `yaml:"mcp"`
}

type resFile struct {
	Label string `yaml:"label"`
	Meta  meta   `yaml:"meta"`
}

// SetMetricsYAML attempts to parse a metrics config to be used by all
// resources.
func (w *ResourcesWrapper) SetMetricsYAML(fileBytes []byte) error {
	return w.builder.SetMetricsYAML(string(fileBytes))
}

// SetTracerYAML attempts to parse a tracer config to be used by all
// resources.
func (w *ResourcesWrapper) SetTracerYAML(fileBytes []byte) error {
	return w.builder.SetTracerYAML(string(fileBytes))
}

func attrString(s trace.Span, key, value string) {
	if len(value) < 128 {
		s.SetAttributes(attribute.String(key, value))
	} else {
		s.SetAttributes(
			attribute.String(key+"_prefix", value[:128]),
			attribute.Int(key+"_length", len(value)),
		)
	}
}

// AddCacheYAML attempts to parse a cache resource config and adds it as an MCP
// tool if appropriate.
func (w *ResourcesWrapper) AddCacheYAML(fileBytes []byte) error {
	var res resFile
	if err := yaml.Unmarshal(fileBytes, &res); err != nil {
		return err
	}

	if !w.labelFilter(res.Label) {
		return nil
	}
	if !w.tagsFilter(res.Meta.Tags) {
		return nil
	}

	if err := w.builder.AddCacheYAML(string(fileBytes)); err != nil {
		return err
	}

	if !res.Meta.MCP.Enabled {
		return nil
	}

	w.logger.With("label", res.Label).Info("Registering cache tools")

	w.svr.AddTool(mcp.NewTool("get-"+res.Label,
		mcp.WithDescription("Obtain an item from "+res.Meta.MCP.Description),
		mcp.WithString("key",
			mcp.Description("The key of the item to obtain."),
			mcp.Required(),
		),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := w.initSpan(ctx, res.Label)
		defer span.End()

		span.SetAttributes(attribute.String("operation", "get"))

		key, exists := request.Params.Arguments["key"].(string)
		if !exists {
			err := errors.New("missing key [string] argument")
			span.RecordError(err)
			return nil, err
		}

		span.SetAttributes(attribute.String("key", key))

		var value []byte
		var getErr error
		if err := w.resources.AccessCache(ctx, res.Label, func(c service.Cache) {
			value, getErr = c.Get(ctx, key)
		}); err != nil {
			span.RecordError(err)
			return nil, err
		}
		if getErr != nil {
			span.RecordError(getErr)
			return nil, getErr
		}

		attrString(span, "value", string(value))

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
		ctx, span := w.initSpan(ctx, res.Label)
		defer span.End()

		span.SetAttributes(attribute.String("operation", "set"))

		key, exists := request.Params.Arguments["key"].(string)
		if !exists {
			err := errors.New("missing key [string] argument")
			span.RecordError(err)
			return nil, err
		}

		span.SetAttributes(attribute.String("key", key))

		value, exists := request.Params.Arguments["value"].(string)
		if !exists {
			err := errors.New("missing value [string] argument")
			span.RecordError(err)
			return nil, err
		}

		attrString(span, "value", value)

		var setErr error
		if err := w.resources.AccessCache(ctx, res.Label, func(c service.Cache) {
			setErr = c.Set(ctx, key, []byte(value), nil)
		}); err != nil {
			span.RecordError(err)
			return nil, err
		}
		if setErr != nil {
			span.RecordError(setErr)
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

// AddInputYAML attempts to parse an input resource config and adds it as an MCP
// tool if appropriate.
func (w *ResourcesWrapper) AddInputYAML(fileBytes []byte) error {
	var res resFile
	if err := yaml.Unmarshal(fileBytes, &res); err != nil {
		return err
	}

	if !w.labelFilter(res.Label) {
		return nil
	}
	if !w.tagsFilter(res.Meta.Tags) {
		return nil
	}

	if err := w.builder.AddInputYAML(string(fileBytes)); err != nil {
		return err
	}

	if !res.Meta.MCP.Enabled {
		return nil
	}

	w.logger.With("label", res.Label).Info("Registering input tool")

	opts := []mcp.ToolOption{
		mcp.WithDescription(res.Meta.MCP.Description),
		mcp.WithNumber("count",
			mcp.Description("The number of messages to read from this input before returning the results."),
			mcp.DefaultNumber(1),
		),
	}

	w.svr.AddTool(mcp.NewTool(res.Label, opts...), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		countFloat, _ := request.Params.Arguments["count"].(float64)

		count := int(countFloat)
		if count <= 0 {
			count = 1
		}

		var resBatch service.MessageBatch
		var iErr error
		if err := w.resources.AccessInput(ctx, res.Label, func(i *service.ResourceInput) {
			for len(resBatch) < count {
				tmpBatch, ackFn, err := i.ReadBatch(ctx)
				if err != nil {
					iErr = err
					return
				}

				// NOTE: We do a deep copy here because after acknowledgement
				// we no longer own the message contents.
				resBatch = append(resBatch, tmpBatch.DeepCopy()...)

				// TODO: Is there a sensible way of hooking up acknowledgements?
				if err := ackFn(ctx, nil); err != nil {
					iErr = err
					return
				}
			}
		}); err != nil {
			return nil, err
		}
		if iErr != nil {
			return nil, iErr
		}

		var content []mcp.Content
		for _, m := range resBatch {
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

// AddProcessorYAML attempts to parse a processor resource config and adds it as
// an MCP tool if appropriate.
func (w *ResourcesWrapper) AddProcessorYAML(fileBytes []byte) error {
	var res resFile
	if err := yaml.Unmarshal(fileBytes, &res); err != nil {
		return err
	}
	if !w.labelFilter(res.Label) {
		return nil
	}
	if !w.tagsFilter(res.Meta.Tags) {
		return nil
	}

	if err := w.builder.AddProcessorYAML(string(fileBytes)); err != nil {
		return err
	}

	if !res.Meta.MCP.Enabled {
		return nil
	}

	w.logger.With("label", res.Label).Info("Registering processor tool")

	opts := []mcp.ToolOption{
		mcp.WithDescription(res.Meta.MCP.Description),
		mcp.WithString("value",
			mcp.Description("The value to execute the tool upon."),
			// mcp.Required(), TODO: Maybe enforce this with no other params?
		),
	}

	extraParams := map[string]bool{}
	for _, p := range res.Meta.MCP.Properties {
		o, err := p.toolOption()
		if err != nil {
			return fmt.Errorf("property '%v': %w", p.Name, err)
		}
		if _, exists := extraParams[p.Name]; exists {
			return fmt.Errorf("duplicate property '%v' detected", p.Name)
		}
		extraParams[p.Name] = p.Required
		opts = append(opts, o)
	}

	w.svr.AddTool(mcp.NewTool(res.Label, opts...), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		value, _ := request.Params.Arguments["value"].(string)
		// TODO: Should we make this required?

		inMsg, span := w.initMsgSpan(res.Label, service.NewMessage([]byte(value)))
		defer span.End()

		attrString(span, "value", value)

		for k, required := range extraParams {
			if v, exists := request.Params.Arguments[k]; exists {
				inMsg.MetaSetMut(k, v)
				attrString(span, k, fmt.Sprintf("%v", v))
			} else if required {
				return nil, fmt.Errorf("required parameter '%v' was missing", k)
			}
		}

		var resBatch service.MessageBatch
		var procErr error
		if err := w.resources.AccessProcessor(ctx, res.Label, func(p *service.ResourceProcessor) {
			resBatch, procErr = p.Process(ctx, inMsg)
		}); err != nil {
			span.RecordError(err)
			return nil, err
		}
		if procErr != nil {
			span.RecordError(procErr)
			return nil, procErr
		}

		var content []mcp.Content
		for _, m := range resBatch {
			if err := m.GetError(); err != nil {
				span.RecordError(err)
				return nil, err
			}

			mBytes, err := m.AsBytes()
			if err != nil {
				span.RecordError(err)
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

// AddOutputYAML attempts to parse an output resource config and adds it as an
// MCP tool if appropriate.
func (w *ResourcesWrapper) AddOutputYAML(fileBytes []byte) error {
	var res resFile
	if err := yaml.Unmarshal(fileBytes, &res); err != nil {
		return err
	}
	if !w.labelFilter(res.Label) {
		return nil
	}
	if !w.tagsFilter(res.Meta.Tags) {
		return nil
	}

	if err := w.builder.AddOutputYAML(string(fileBytes)); err != nil {
		return err
	}

	if !res.Meta.MCP.Enabled {
		return nil
	}

	w.logger.With("label", res.Label).Info("Registering output tool")

	messageProperties := map[string]any{
		"value": map[string]any{
			"type":        "string",
			"description": "The raw contents of the message",
			"required":    true,
		},
	}

	for _, p := range res.Meta.MCP.Properties {
		o, err := p.rawOption()
		if err != nil {
			return fmt.Errorf("property '%v': %w", p.Name, err)
		}
		if _, exists := messageProperties[p.Name]; exists {
			return fmt.Errorf("duplicate property '%v' detected", p.Name)
		}
		messageProperties[p.Name] = o
	}

	opts := []mcp.ToolOption{
		mcp.WithDescription(res.Meta.MCP.Description),
		mcp.WithArray("messages",
			mcp.Required(),
			mcp.Items(map[string]any{
				"type":       "object",
				"properties": messageProperties,
			}),
		),
	}

	w.svr.AddTool(mcp.NewTool(res.Label, opts...), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		messages, exists := request.Params.Arguments["messages"].([]any)
		if !exists || len(messages) == 0 {
			return nil, errors.New("at least one message is required")
		}

		var spans []trace.Span

		var inBatch service.MessageBatch
		for i, m := range messages {
			mObj, ok := m.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("message %v was not an object", i)
			}

			contents, exists := mObj["value"].(string)
			if !exists {
				return nil, fmt.Errorf("message %v is missing a value", i)
			}

			msg, span := w.initMsgSpan(res.Label, service.NewMessage([]byte(contents)))
			defer span.End()

			attrString(span, "contents", contents)

			for k, v := range mObj {
				if k == "value" {
					continue
				}
				msg.MetaSetMut(k, v)
				attrString(span, k, fmt.Sprintf("%v", v))
			}

			spans = append(spans, span)
			inBatch = append(inBatch, msg)
		}

		var outErr error
		if err := w.resources.AccessOutput(ctx, res.Label, func(o *service.ResourceOutput) {
			outErr = o.WriteBatch(ctx, inBatch)
		}); err != nil {
			for _, s := range spans {
				s.RecordError(err)
			}
			return nil, err
		}
		if outErr != nil {
			for _, s := range spans {
				s.RecordError(outErr)
			}
			return nil, outErr
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				mcp.TextContent{
					Type: "text",
					Text: "Messages delivered successfully",
				},
			},
		}, nil
	})

	return nil
}
