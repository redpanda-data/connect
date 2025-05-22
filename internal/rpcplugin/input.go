// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcplugin

import (
	"context"
	"errors"
	"fmt"

	"github.com/cenkalti/backoff/v4"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/subprocess"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InputConfig is the configuration for a plugin input.
type InputConfig struct {
	// The name of the plugin
	Name string
	// The command to run the plugin process
	Cmd []string
	// The environment variables to set for the plugin process
	//
	// This does NOT inherit from the current process
	Env map[string]string
	// Directory for the process
	Cwd string
	// The configuration spec for the plugin
	Spec *service.ConfigSpec
}

type input struct {
	cfgValue any
	proc     *subprocess.Subprocess
	client   runtimepb.BatchInputServiceClient
}

var _ service.BatchInput = (*input)(nil)

// RegisterInputPlugin creates a new input plugin from the configuration.
func RegisterInputPlugin(env *service.Environment, spec InputConfig) error {
	if len(spec.Cmd) == 0 {
		return errors.New("plugin command is required")
	}
	ctor := func(parsed *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
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
		proc, err := subprocess.New(
			spec.Cmd,
			spec.Env,
			subprocess.WithLogger(res.Logger()),
			subprocess.WithCwd(spec.Cwd),
		)
		if err != nil {
			return nil, fmt.Errorf("invalid subprocess: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), maxStartupTime)
		defer cancel()
		client := runtimepb.NewBatchInputServiceClient(conn)
		autoRetryNacks, err := startInputPlugin(ctx, proc, client, cfgValue)
		if err != nil {
			return nil, fmt.Errorf("unable to restart plugin: %w", err)
		}
		i := &input{
			cfgValue: cfgValue,
			proc:     proc,
			client:   client,
		}
		cleanup = nil // Prevent cleanup from running.
		if autoRetryNacks {
			return service.AutoRetryNacksBatched(i), nil
		}
		return i, nil
	}
	return env.RegisterBatchInput(spec.Name, spec.Spec, ctor)
}

func startInputPlugin(
	ctx context.Context,
	proc *subprocess.Subprocess,
	client runtimepb.BatchInputServiceClient,
	cfgValue any,
) (autoRetryNacks bool, err error) {
	if err := proc.Start(); err != nil {
		if errors.Is(err, subprocess.ErrProcessAlreadyStarted) {
			return false, nil
		}
		return false, fmt.Errorf("unable to restart plugin: %w", err)
	}
	value, err := runtimepb.AnyToProto(cfgValue)
	if err != nil {
		_ = proc.Close(ctx)
		return false, fmt.Errorf("unable to convert config to proto: %w", err)
	}
	// Retry to wait for the process to start
	autoRetryNacks, err = backoff.RetryWithData(func() (bool, error) {
		resp, err := client.Init(ctx, &runtimepb.BatchInputInitRequest{
			Config: value,
		})
		if err != nil {
			if !proc.IsRunning() {
				return false, backoff.Permanent(fmt.Errorf("plugin exited early: %w", err))
			}
			return false, err
		}
		if err = runtimepb.ProtoToError(resp.Error); err != nil {
			return false, backoff.Permanent(err)
		}
		return resp.AutoReplayNacks, nil
	}, backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(maxStartupTime)))
	if err != nil {
		_ = proc.Close(ctx)
		return false, fmt.Errorf("unable to initialize plugin: %w", err)
	}
	return autoRetryNacks, nil
}

// Connect implements service.BatchInput.
func (i *input) Connect(ctx context.Context) (err error) {
	var resp *runtimepb.BatchInputConnectResponse
	// If the plugin crashes attempt to restart the process up to retryCount times.
	for range retryCount {
		resp, err = i.client.Connect(ctx, &runtimepb.BatchInputConnectRequest{})
		if err != nil {
			err = fmt.Errorf("unable to reach plugin: %w", err)
			if i.proc.IsRunning() {
				return err
			}
			if err := i.proc.Close(ctx); err != nil {
				return fmt.Errorf("unable to restart plugin process: %w", err)
			}
			if _, err := startInputPlugin(ctx, i.proc, i.client, i.cfgValue); err != nil {
				return fmt.Errorf("unable to restart plugin: %w", err)
			}
			continue
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to connect to plugin: %w", err)
	}
	return runtimepb.ProtoToError(resp.Error)
}

// ReadBatch implements service.BatchInput.
func (i *input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	resp, err := i.client.ReadBatch(ctx, &runtimepb.BatchInputReadRequest{})
	if err != nil {
		if !i.proc.IsRunning() {
			return nil, nil, service.ErrNotConnected
		}
		return nil, nil, fmt.Errorf("unable to read from plugin: %w", err)
	}
	if err := runtimepb.ProtoToError(resp.Error); err != nil {
		return nil, nil, err
	}
	id := resp.BatchId
	batch, err := runtimepb.ProtoToMessageBatch(resp.Batch)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to convert batch from proto: %w", err)
	}
	return batch, func(ctx context.Context, err error) error {
		resp, err := i.client.Ack(ctx, &runtimepb.BatchInputAckRequest{
			BatchId: id,
			Error:   runtimepb.ErrorToProto(err),
		})
		if err != nil {
			return fmt.Errorf("unable to ack batch with ID %d: %w", id, err)
		}
		return runtimepb.ProtoToError(resp.Error)
	}, nil
}

// Close implements service.BatchInput.
func (i *input) Close(ctx context.Context) error {
	resp, err := i.client.Close(ctx, &runtimepb.BatchInputCloseRequest{})
	if err != nil {
		return fmt.Errorf("unable to close plugin: %w", err)
	}
	if err := runtimepb.ProtoToError(resp.Error); err != nil {
		return fmt.Errorf("plugin close error: %w", err)
	}
	if err := i.proc.Close(ctx); err != nil {
		return fmt.Errorf("unable to close plugin process: %w", err)
	}
	return nil
}
