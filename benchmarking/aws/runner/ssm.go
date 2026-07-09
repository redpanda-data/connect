// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

// SSMExecutor executes shell commands on EC2 instances via Systems Manager.
type SSMExecutor interface {
	// Run executes a script on the named instance, streaming stdout line-by-line
	// to onLine until the command finishes or ctx is cancelled.
	Run(ctx context.Context, instanceID, script string, onLine func(string)) error
}

type awsSSM struct {
	client *ssm.Client
}

// NewSSMExecutor builds an executor backed by the AWS SDK in the given region.
func NewSSMExecutor(ctx context.Context, region string) (SSMExecutor, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &awsSSM{client: ssm.NewFromConfig(cfg)}, nil
}

func (a *awsSSM) Run(ctx context.Context, instanceID, script string, onLine func(string)) error {
	send, err := a.client.SendCommand(ctx, &ssm.SendCommandInput{
		InstanceIds:  []string{instanceID},
		DocumentName: aws.String("AWS-RunShellScript"),
		Parameters:   map[string][]string{"commands": {script}},
		TimeoutSeconds: aws.Int32(int32((90 * time.Minute).Seconds())),
	})
	if err != nil {
		return fmt.Errorf("send command: %w", err)
	}
	commandID := *send.Command.CommandId

	var lastSeen int
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_, _ = a.client.CancelCommand(context.Background(), &ssm.CancelCommandInput{
				CommandId:   aws.String(commandID),
				InstanceIds: []string{instanceID},
			})
			return ctx.Err()
		case <-ticker.C:
		}
		inv, err := a.client.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
			CommandId:  aws.String(commandID),
			InstanceId: aws.String(instanceID),
		})
		if err != nil {
			// Not yet propagated; keep polling.
			continue
		}
		stdout := aws.ToString(inv.StandardOutputContent)
		if len(stdout) > lastSeen && onLine != nil {
			emit := stdout[lastSeen:]
			for _, line := range strings.Split(strings.TrimRight(emit, "\n"), "\n") {
				if line != "" {
					onLine(line)
				}
			}
			lastSeen = len(stdout)
		}
		switch inv.Status {
		case types.CommandInvocationStatusSuccess:
			return nil
		case types.CommandInvocationStatusFailed,
			types.CommandInvocationStatusCancelled,
			types.CommandInvocationStatusTimedOut:
			return fmt.Errorf("ssm command %s on %s ended with status %s: %s",
				commandID, instanceID, inv.Status, aws.ToString(inv.StandardErrorContent))
		}
	}
}

// FakeSSM is a deterministic SSMExecutor for tests — emits a scripted
// transcript and never touches AWS.
type FakeSSM struct {
	Transcripts map[string][]string // instanceID → lines to emit on Run
	Errs        map[string]error
}

func (f *FakeSSM) Run(_ context.Context, instanceID, _ string, onLine func(string)) error {
	for _, line := range f.Transcripts[instanceID] {
		if onLine != nil {
			onLine(line)
		}
	}
	return f.Errs[instanceID]
}

// streamingOnLine forwards each line to a writer, prefixing with [instance:].
func streamingOnLine(w io.Writer, prefix string) func(string) {
	return func(line string) {
		fmt.Fprintf(w, "[%s] %s\n", prefix, line)
	}
}
