/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/dynamic/plugin/runtimepb"
	"github.com/redpanda-data/connect/v4/internal/dynamic/plugin/subprocess"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InputConfig is the configuration for a plugin input.
type ProcessorConfig struct {
	// The name of the plugin
	Name string
	// The command to run the plugin process
	Cmd []string
	// The environment variables to set for the plugin process
	//
	// This does NOT inherit from the current process
	Env map[string]string
	// The configuration spec for the plugin
	Spec *service.ConfigSpec
}

type processor struct {
	cfgValue any
	proc     subprocess.SubProcess
	client   runtimepb.BatchProcessorServiceClient
	mu       sync.Mutex
}

var _ service.BatchProcessor = (*processor)(nil)

// RegisterProcessorPlugin creates a new input plugin from the configuration.
func RegisterProcessorPlugin(env *service.Environment, spec ProcessorConfig) error {
	if len(spec.Cmd) == 0 {
		return errors.New("plugin command is required")
	}
	ctor := func(parsed *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
		cfgValue, err := parsed.FieldAny()
		if err != nil {
			return nil, err
		}
		if spec.Env == nil {
			spec.Env = make(map[string]string)
		}
		socketPath, err := newUnixSocketAddr()
		if err != nil {
			return nil, err
		}
		var cleanup []func() error
		defer func() {
			for _, fn := range cleanup {
				err := fn()
				if err != nil {
					res.Logger().Warnf("failed to clean up creating %s: %v", spec.Name, err)
				}
			}
		}()
		// No I/O happens in NewClient, so we can do this before we start the subprocess.
		// This simplifies the cleanup if there is a failure.
		conn, err := grpc.NewClient(
			socketPath,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		cleanup = append(cleanup, conn.Close)
		spec.Env["REDPANDA_CONNECT_PLUGIN_ADDRESS"] = socketPath
		proc, err := subprocess.New(spec.Cmd, spec.Env, subprocess.WithLogger(res.Logger()))
		if err := proc.Start(); err != nil {
			return nil, fmt.Errorf("unable to start subprocess: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), maxStartupTime)
		defer cancel()
		cleanup = append(cleanup, func() error {
			return proc.Close(ctx)
		})
		client := runtimepb.NewBatchProcessorServiceClient(conn)
		err = startProcessorPlugin(ctx, proc, client, cfgValue)
		if err != nil {
			return nil, fmt.Errorf("unable to restart plugin: %w", err)
		}
		p := &processor{
			cfgValue: cfgValue,
			proc:     proc,
			client:   client,
		}
		cleanup = nil // Prevent cleanup from running.
		return p, nil
	}
	return env.RegisterBatchProcessor(spec.Name, spec.Spec, ctor)
}

func startProcessorPlugin(
	ctx context.Context,
	proc subprocess.SubProcess,
	client runtimepb.BatchProcessorServiceClient,
	cfgValue any,
) (err error) {
	if err := proc.Start(); err != nil {
		return fmt.Errorf("unable to restart plugin: %w", err)
	}
	value, err := runtimepb.AnyToProto(cfgValue)
	if err != nil {
		return fmt.Errorf("unable to convert config to proto: %w", err)
	}
	err = backoff.Retry(func() error {
		resp, err := client.Init(ctx, &runtimepb.BatchProcessorInitRequest{
			Config: value,
		})
		if err != nil {
			if !proc.IsRunning() {
				return backoff.Permanent(fmt.Errorf("plugin exited early: %w", err))
			}
			return err
		}
		return runtimepb.ProtoToError(resp.Error)
	}, backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(maxStartupTime)))
	if err != nil {
		return fmt.Errorf("unable to initialize plugin: %w", err)
	}
	return nil
}

// ProcessBatch implements service.BatchProcessor.
func (p *processor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	proto, err := runtimepb.MessageBatchToProto(batch)
	if err != nil {
		return nil, fmt.Errorf("unable to convert batch to proto: %w", err)
	}
	var resp *runtimepb.BatchProcessorProcessBatchResponse
	for range retryCount {
		resp, err = p.client.ProcessBatch(ctx, &runtimepb.BatchProcessorProcessBatchRequest{
			Batch: proto,
		})
		if err != nil {
			if p.proc.IsRunning() {
				return nil, fmt.Errorf("unable to read from plugin: %w", err)
			}
			// Otherwise we assume the process might have crashed, so attempt to restart it
			p.mu.Lock()
			if !p.proc.IsRunning() {
				err = startProcessorPlugin(ctx, p.proc, p.client, p.cfgValue)
			}
			p.mu.Unlock()
			if err != nil {
				return nil, fmt.Errorf("unable to restart plugin: %w", err)
			}
		}
		break
	}
	if err := runtimepb.ProtoToError(resp.Error); err != nil {
		return nil, err
	}
	batches := make([]service.MessageBatch, 0, len(resp.Batches))
	for _, proto := range resp.Batches {
		batch, err := runtimepb.ProtoToMessageBatch(proto)
		if err != nil {
			return nil, fmt.Errorf("unable to convert batch from proto: %w", err)
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// Close implements service.BatchProcessor.
func (p *processor) Close(ctx context.Context) error {
	resp, err := p.client.Close(ctx, &runtimepb.BatchProcessorCloseRequest{})
	if err != nil {
		return fmt.Errorf("unable to close plugin: %w", err)
	}
	if err := runtimepb.ProtoToError(resp.Error); err != nil {
		return fmt.Errorf("plugin close error: %w", err)
	}
	return p.proc.Close(ctx)
}
