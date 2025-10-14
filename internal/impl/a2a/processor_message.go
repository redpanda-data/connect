// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package a2a

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2aclient/agentcard"
	"golang.org/x/oauth2"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	ampFieldAgentCardURL     = "agent_card_url"
	ampFieldPrompt           = "prompt"
	ampFieldFinalMessageOnly = "final_message_only"
)

func init() {
	service.MustRegisterProcessor(
		"a2a_message",
		processorConfig(),
		makeProcessor,
	)
}

func processorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI").
		Summary("Sends messages to an A2A (Agent-to-Agent) protocol agent and returns the response.").
		Description(`
This processor enables Redpanda Connect pipelines to communicate with A2A protocol agents. Currently only JSON-RPC transport is supported.

The processor sends a message to the agent and polls for task completion. The agent's response
is returned as the processor output.

For more information about the A2A protocol, see https://a2a-protocol.org/latest/specification`).
		Version("4.40.0").
		Fields(
			service.NewURLField(ampFieldAgentCardURL).
				Description("URL for the A2A agent card. Can be either a base URL (e.g., `https://example.com`) or a full path to the agent card (e.g., `https://example.com/.well-known/agent.json`). If no path is provided, defaults to `/.well-known/agent.json`. Authentication uses OAuth2 from environment variables."),
			service.NewInterpolatedStringField(ampFieldPrompt).
				Description("The user prompt to send to the agent. By default, the processor submits the entire payload as a string.").
				Default(""),
			service.NewBoolField(ampFieldFinalMessageOnly).
				Description("If true, returns only the text from the final agent message (concatenated from all text parts). If false, returns the complete Message or Task object as structured data with full history, artifacts, and metadata.").
				Default(true).
				Advanced(),
		)
}

type messageProcessor struct {
	agentCardURL     string
	agentURL         string
	prompt           *service.InterpolatedString
	finalMessageOnly bool
	client           *a2aclient.Client
	tokenSource      oauth2.TokenSource
	logger           *service.Logger
}

func makeProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, fmt.Errorf("a2a_message processor requires a valid license: %w", err)
	}

	agentCardURL, err := conf.FieldString(ampFieldAgentCardURL)
	if err != nil {
		return nil, err
	}

	prompt, err := conf.FieldInterpolatedString(ampFieldPrompt)
	if err != nil {
		return nil, err
	}

	finalMessageOnly, err := conf.FieldBool(ampFieldFinalMessageOnly)
	if err != nil {
		return nil, err
	}

	oauth2Cfg, err := NewOAuth2ConfigFromEnv()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	// Create authenticated HTTP client
	httpClient, tokenSource, err := oauth2Cfg.CreateHTTPClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create OAuth2 HTTP client: %w", err)
	}

	// Fetch agent card to discover the actual agent endpoint URL
	// Note: We use OAuth2 auth to fetch the card, but ignore card's security schemes
	token, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to get OAuth2 token for agent card fetch: %w", err)
	}

	// Parse the agent card URL to separate base URL and path
	baseURL, cardPath := parseAgentCardURL(agentCardURL)

	resolver := &agentcard.Resolver{BaseURL: baseURL}
	card, err := resolver.Resolve(ctx,
		agentcard.WithPath(cardPath),
		agentcard.WithRequestHeader("Authorization", "Bearer "+token.AccessToken))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch agent card from %s: %w", agentCardURL, err)
	}

	mgr.Logger().Debugf("Fetched agent card: %s (version: %s, protocol: %s)", card.Name, card.Version, card.ProtocolVersion)

	// Extract the actual agent URL from the card
	agentURL := card.URL
	if agentURL == "" {
		return nil, errors.New("agent card does not contain a URL")
	}

	// Create HTTP transport factory
	transportFactory := a2aclient.TransportFactoryFn(func(_ context.Context, url string, card *a2a.AgentCard) (a2aclient.Transport, error) {
		return NewHTTPTransport(url, httpClient), nil
	})

	// Create OAuth2 bearer interceptor
	oauth2Interceptor := &oauth2BearerInterceptor{
		tokenSource: tokenSource,
	}

	// Create A2A client factory
	factory := a2aclient.NewFactory(
		a2aclient.WithDefaultsDisabled(),
		a2aclient.WithTransport(a2a.TransportProtocolJSONRPC, transportFactory),
		a2aclient.WithInterceptors(oauth2Interceptor),
	)

	// Create client from endpoint (use URL from agent card)
	client, err := factory.CreateFromEndpoints(ctx, []a2a.AgentInterface{
		{
			Transport: a2a.TransportProtocolJSONRPC,
			URL:       agentURL,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create A2A client: %w", err)
	}

	return &messageProcessor{
		agentCardURL:     agentCardURL,
		agentURL:         agentURL,
		prompt:           prompt,
		finalMessageOnly: finalMessageOnly,
		client:           client,
		tokenSource:      tokenSource,
		logger:           mgr.Logger(),
	}, nil
}

func (p *messageProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Get prompt text
	promptText, err := p.prompt.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate prompt: %w", err)
	}

	// If prompt is empty, use message payload as string
	if promptText == "" {
		payloadBytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to get message payload: %w", err)
		}
		promptText = string(payloadBytes)
	}

	p.logger.Debugf("Processing A2A request with prompt: %q", promptText)

	// Create A2A message
	a2aMessage := a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: promptText})

	// Send message
	p.logger.Debugf("Sending message/send to agent: %s", p.agentURL)
	result, err := p.client.SendMessage(ctx, &a2a.MessageSendParams{
		Message: a2aMessage,
	})
	if err != nil {
		p.logger.Errorf("Failed to send A2A message: %v", err)
		return nil, fmt.Errorf("failed to send A2A message: %w", err)
	}

	// Handle result
	switch r := result.(type) {
	case *a2a.Task:
		p.logger.Debugf("Received Task response: ID=%s, Status=%s", r.ID, r.Status.State)
		return p.handleTaskResult(ctx, r)
	case *a2a.Message:
		p.logger.Debugf("Received Message response: ID=%s", r.ID)
		return p.handleMessageResult(r)
	default:
		return nil, fmt.Errorf("unexpected result type: %T", r)
	}
}

func (p *messageProcessor) handleTaskResult(ctx context.Context, task *a2a.Task) (service.MessageBatch, error) {
	// Poll for task completion if not terminal
	if !task.Status.State.Terminal() {
		p.logger.Debugf("Task %s in state %s, starting polling for completion...", task.ID, task.Status.State)
		finalTask, err := p.pollTaskUntilComplete(ctx, task.ID)
		if err != nil {
			p.logger.Errorf("Task polling failed: %v", err)
			return nil, err
		}
		task = finalTask
	} else {
		p.logger.Debugf("Task %s already in terminal state: %s", task.ID, task.Status.State)
	}

	// Only return output if task completed successfully
	if task.Status.State != a2a.TaskStateCompleted {
		p.logger.Warnf("Task %s ended in non-completed state: %s (not returning output)", task.ID, task.Status.State)
		return nil, fmt.Errorf("task %s ended in state %s (expected completed)", task.ID, task.Status.State)
	}

	p.logger.Debugf("Task %s has %d messages in history, %d artifacts", task.ID, len(task.History), len(task.Artifacts))

	outMsg := service.NewMessage(nil)
	outMsg.MetaSetMut("a2a_task_id", string(task.ID))
	outMsg.MetaSetMut("a2a_context_id", task.ContextID)
	outMsg.MetaSetMut("a2a_state", string(task.Status.State))

	if p.finalMessageOnly {
		// Extract text from last agent message only
		var responseText strings.Builder
		var lastAgentMessage *a2a.Message

		p.logger.Debugf("Extracting final message only from task %s (total history: %d messages)", task.ID, len(task.History))

		// Log all history for debugging
		for i, histMsg := range task.History {
			p.logger.Debugf("  History[%d]: Role=%s, MessageID=%s, Parts=%d", i, histMsg.Role, histMsg.ID, len(histMsg.Parts))
		}

		for i := len(task.History) - 1; i >= 0; i-- {
			if task.History[i].Role == a2a.MessageRoleAgent {
				lastAgentMessage = task.History[i]
				p.logger.Debugf("Found last agent message at history index %d (MessageID=%s)", i, lastAgentMessage.ID)
				break
			}
		}

		if lastAgentMessage != nil {
			p.logger.Debugf("Last agent message has %d parts", len(lastAgentMessage.Parts))
			for i, part := range lastAgentMessage.Parts {
				if textPart, ok := part.(a2a.TextPart); ok {
					p.logger.Debugf("  Part %d: text with %d chars", i, len(textPart.Text))
					if responseText.Len() > 0 {
						responseText.WriteString("\n")
					}
					responseText.WriteString(textPart.Text)
				} else {
					p.logger.Debugf("  Part %d: %T (skipped)", i, part)
				}
			}
		}

		if responseText.Len() == 0 {
			p.logger.Errorf("No text found in last agent message for task %s", task.ID)
			return nil, errors.New("agent response contained no text")
		}

		outMsg.SetBytes([]byte(responseText.String()))
		p.logger.Debugf("Task %s completed, returning ONLY final message text (%d bytes total)", task.ID, responseText.Len())
	} else {
		// Return the complete Task as a structured object
		outMsg.SetStructuredMut(task)
		p.logger.Debugf("Task %s completed, returning full task object (history: %d msgs, artifacts: %d)",
			task.ID, len(task.History), len(task.Artifacts))
	}

	return service.MessageBatch{outMsg}, nil
}

func (p *messageProcessor) handleMessageResult(msg *a2a.Message) (service.MessageBatch, error) {
	outMsg := service.NewMessage(nil)
	outMsg.MetaSetMut("a2a_message_id", msg.ID)
	if msg.ContextID != "" {
		outMsg.MetaSetMut("a2a_context_id", msg.ContextID)
	}
	if msg.TaskID != "" {
		outMsg.MetaSetMut("a2a_task_id", string(msg.TaskID))
	}

	if p.finalMessageOnly {
		// Extract and return text only
		var responseText strings.Builder
		for _, part := range msg.Parts {
			if textPart, ok := part.(a2a.TextPart); ok {
				if responseText.Len() > 0 {
					responseText.WriteString("\n")
				}
				responseText.WriteString(textPart.Text)
			}
		}

		if responseText.Len() == 0 {
			return nil, errors.New("agent message contained no text")
		}

		outMsg.SetBytes([]byte(responseText.String()))
		p.logger.Debugf("Returning message text only (%d bytes)", responseText.Len())
	} else {
		// Return the complete Message as a structured object
		outMsg.SetStructuredMut(msg)
		p.logger.Debugf("Returning full message object (%d parts)", len(msg.Parts))
	}

	return service.MessageBatch{outMsg}, nil
}

func (p *messageProcessor) pollTaskUntilComplete(ctx context.Context, taskID a2a.TaskID) (*a2a.Task, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeout := time.After(5 * time.Minute)
	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			p.logger.Debugf("Context cancelled while waiting for task %s (polled %d times)", taskID, pollCount)
			return nil, ctx.Err()

		case <-timeout:
			p.logger.Errorf("Timeout after 5 minutes waiting for task %s (polled %d times)", taskID, pollCount)
			return nil, fmt.Errorf("timeout waiting for task %s to complete", taskID)

		case <-ticker.C:
			pollCount++
			p.logger.Debugf("Polling task %s (attempt %d) via tasks/get...", taskID, pollCount)

			task, err := p.client.GetTask(ctx, &a2a.TaskQueryParams{
				ID: taskID,
			})
			if err != nil {
				p.logger.Errorf("Failed to get task status on poll %d: %v", pollCount, err)
				return nil, fmt.Errorf("failed to get task status: %w", err)
			}

			p.logger.Debugf("Task %s poll %d: state=%s", taskID, pollCount, task.Status.State)

			// Log status message if present
			if task.Status.Message != nil && len(task.Status.Message.Parts) > 0 {
				for _, part := range task.Status.Message.Parts {
					if textPart, ok := part.(a2a.TextPart); ok {
						preview := textPart.Text
						if len(preview) > 100 {
							preview = preview[:100] + "..."
						}
						p.logger.Debugf("  Status message: %s", preview)
					}
				}
			}

			if task.Status.State.Terminal() {
				p.logger.Debugf("Task %s reached terminal state %s after %d polls", taskID, task.Status.State, pollCount)
				return task, nil
			}
		}
	}
}

func (p *messageProcessor) Close(_ context.Context) error {
	if p.client != nil {
		return p.client.Destroy()
	}
	return nil
}

// parseAgentCardURL separates a URL into base URL and path.
// If the URL contains a path component (e.g., /.well-known/agent.json), returns the base and path separately.
// Otherwise returns the URL as base and "/.well-known/agent.json" as default path.
func parseAgentCardURL(fullURL string) (baseURL, path string) {
	// Check if URL contains /.well-known or similar path
	if idx := strings.Index(fullURL, "/.well-known"); idx != -1 {
		return fullURL[:idx], fullURL[idx:]
	}
	// Default path if no path component found
	return fullURL, "/.well-known/agent.json"
}
