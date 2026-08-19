// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

// defaultExecTimeout is the AWS-RunShellScript document's own
// executionTimeout default (distinct from SendCommand's delivery
// TimeoutSeconds below). Used when NewSSMExecutor is given a zero
// execTimeout, which reproduces the implicit behavior every caller relied on
// before this field existed: any script running past 3600s was silently
// killed by the SSM agent — the reason nothing longer than the ~17min sweep
// points ever surfaced the bug.
const defaultExecTimeout = time.Hour

// sendCommandDeliveryTimeout bounds how long SendCommand waits for the
// instance to START the command, not how long the command may run. It is
// unrelated to execTimeout and does not need to scale with run length.
const sendCommandDeliveryTimeout = 90 * time.Minute

// pollTickInterval is how often Run polls GetCommandInvocation once a
// command has been sent. Named so the two error-tolerance caps below can
// express their sizing in real time rather than a bare tick count.
const pollTickInterval = 2 * time.Second

// notYetPropagatedMaxPolls bounds how many CONSECUTIVE
// types.InvocationDoesNotExist responses Run tolerates before treating
// them as a real failure instead of eventual-consistency lag between
// SendCommand succeeding and the invocation record becoming visible.
// SendCommand has already succeeded by the time polling starts, so the
// invocation still not existing after this long means something is
// genuinely wrong (e.g. the instance was terminated out from under the
// command) — fail rather than poll forever.
const notYetPropagatedMaxPolls = 150 // 150 * pollTickInterval == 5 minutes

// otherPollErrMaxPolls bounds how many CONSECUTIVE poll errors OTHER than
// InvocationDoesNotExist (e.g. a transient network blip, an API throttle,
// or a persistent failure like expired STS credentials) Run tolerates
// before giving up. Smaller than notYetPropagatedMaxPolls because these
// aren't an expected part of the command's lifecycle the way propagation
// lag is — a SHORT transient blip is plausible, but a failure that
// persists this long is not going to self-resolve, and without this cap a
// 24h soak with no external timeout would otherwise poll forever against
// paid, idle EC2/RDS.
const otherPollErrMaxPolls = 30 // 30 * pollTickInterval == 1 minute

// SSMExecutor executes shell commands on EC2 instances via Systems Manager.
type SSMExecutor interface {
	// Run executes a script on the named instance, streaming stdout line-by-line
	// to onLine until the command finishes or ctx is cancelled.
	Run(ctx context.Context, instanceID, script string, onLine func(string)) error
}

type awsSSM struct {
	client *ssm.Client
	// execTimeout is the AWS-RunShellScript document's executionTimeout
	// parameter: how long the SSM agent lets the script itself run before
	// killing it. Distinct from sendCommandDeliveryTimeout (time-to-start).
	execTimeout time.Duration
}

// NewSSMExecutor builds an executor backed by the AWS SDK in the given
// region. execTimeout bounds how long a submitted script may run before the
// SSM agent kills it (the AWS-RunShellScript document's executionTimeout,
// which otherwise defaults to 3600s); 0 keeps that historical default. All
// scripts submitted through one executor share this cap, so a caller with a
// long-running soak point should size it generously — a large cap is
// harmless for the short staging/seed commands that share the same executor.
func NewSSMExecutor(ctx context.Context, region string, execTimeout time.Duration) (SSMExecutor, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	if execTimeout <= 0 {
		execTimeout = defaultExecTimeout
	}
	return &awsSSM{client: ssm.NewFromConfig(cfg), execTimeout: execTimeout}, nil
}

// classifyPollError decides whether one GetCommandInvocation poll error is
// worth retrying (a benign, expected part of the command's lifecycle) or
// should end the poll loop outright, given the CONSECUTIVE occurrence
// counts carried over from the previous poll. err == nil (the poll
// succeeded) always resets both counters to 0 and reports keepPolling —
// there is nothing to retry, but callers still route success through here
// so the reset lives in one place.
//
// types.InvocationDoesNotExist is treated as benign propagation lag (see
// notYetPropagatedMaxPolls) and given a much longer leash than any other
// error (see otherPollErrMaxPolls), which includes both one-off transient
// blips and a persistent failure like expired STS credentials — either of
// which should surface as a real error in bounded time rather than poll
// forever against paid, idle infrastructure.
//
// Pulled out as a pure function, rather than inlined in Run's loop, so the
// decision boundary can be table-tested without a fake SSM client — Run
// itself has no test seam for the real AWS SDK client.
func classifyPollError(err error, notYetPropagatedCount, otherErrCount int) (keepPolling bool, newNotYetPropagatedCount, newOtherErrCount int) {
	if err == nil {
		return true, 0, 0
	}
	var notFound *types.InvocationDoesNotExist
	if errors.As(err, &notFound) {
		notYetPropagatedCount++
		return notYetPropagatedCount <= notYetPropagatedMaxPolls, notYetPropagatedCount, otherErrCount
	}
	otherErrCount++
	return otherErrCount <= otherPollErrMaxPolls, notYetPropagatedCount, otherErrCount
}

func (a *awsSSM) Run(ctx context.Context, instanceID, script string, onLine func(string)) error {
	send, err := a.client.SendCommand(ctx, &ssm.SendCommandInput{
		InstanceIds:  []string{instanceID},
		DocumentName: aws.String("AWS-RunShellScript"),
		Parameters: map[string][]string{
			"commands":         {script},
			"executionTimeout": {strconv.Itoa(int(a.execTimeout.Seconds()))},
		},
		TimeoutSeconds: aws.Int32(int32(sendCommandDeliveryTimeout.Seconds())),
	})
	if err != nil {
		return fmt.Errorf("send command: %w", err)
	}
	commandID := *send.Command.CommandId

	var lastSeen int
	// notYetPropagatedCount and otherErrCount are CONSECUTIVE occurrence
	// counters, each reset to 0 the moment a poll succeeds — see
	// classifyPollError. Tracked separately so a run of one error kind
	// doesn't get an unearned reprieve from being interleaved with the
	// other.
	var notYetPropagatedCount, otherErrCount int
	ticker := time.NewTicker(pollTickInterval)
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
		var keepPolling bool
		keepPolling, notYetPropagatedCount, otherErrCount = classifyPollError(err, notYetPropagatedCount, otherErrCount)
		if err != nil {
			if !keepPolling {
				return fmt.Errorf("get command invocation for command %s on instance %s (gave up after repeated poll errors): %w",
					commandID, instanceID, err)
			}
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
	// mu guards Scripts: MatrixRunner.Run drives the workload script on a
	// separate goroutine concurrently with the bench script, so two Run
	// calls can append at the same time.
	mu sync.Mutex
	// Scripts records every script submitted, in order, so tests can assert on
	// what the runner actually asked the host to execute.
	Scripts []string
}

func (f *FakeSSM) Run(_ context.Context, instanceID, script string, onLine func(string)) error {
	f.mu.Lock()
	f.Scripts = append(f.Scripts, script)
	f.mu.Unlock()
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
