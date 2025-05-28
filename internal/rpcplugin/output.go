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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/subprocess"
)

// OutputConfig is the configuration for a plugin output.
type OutputConfig struct {
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

type output struct {
	cfgValue any
	proc     *subprocess.Subprocess
	client   runtimepb.BatchOutputServiceClient
}

var _ service.BatchOutput = (*output)(nil)

// RegisterOutputPlugin creates a new input plugin from the configuration.
func RegisterOutputPlugin(env *service.Environment, spec OutputConfig) error {
	if len(spec.Cmd) == 0 {
		return errors.New("plugin command is required")
	}
	ctor := func(parsed *service.ParsedConfig, res *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
		cfgValue, err := parsed.FieldAny()
		if err != nil {
			return nil, service.BatchPolicy{}, 0, err
		}
		if spec.Env == nil {
			spec.Env = make(map[string]string)
		}
		socketPath, err := newUnixSocketAddr()
		if err != nil {
			return nil, service.BatchPolicy{}, 0, err
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
			return nil, service.BatchPolicy{}, 0, err
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
			err = fmt.Errorf("invalid subprocess: %w", err)
			return nil, service.BatchPolicy{}, 0, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), maxStartupTime)
		defer cancel()
		client := runtimepb.NewBatchOutputServiceClient(conn)
		maxInFlight, batchPolicy, err := startOutputPlugin(ctx, proc, client, cfgValue)
		if err != nil {
			err = fmt.Errorf("unable to restart plugin: %w", err)
			return nil, service.BatchPolicy{}, 0, err
		}
		o := &output{
			cfgValue: cfgValue,
			proc:     proc,
			client:   client,
		}
		cleanup = nil // Prevent cleanup from running.
		return o, batchPolicy, maxInFlight, nil
	}
	return env.RegisterBatchOutput(spec.Name, spec.Spec, ctor)
}

func startOutputPlugin(
	ctx context.Context,
	proc *subprocess.Subprocess,
	client runtimepb.BatchOutputServiceClient,
	cfgValue any,
) (maxInFlight int, batchPolicy service.BatchPolicy, err error) {
	if err := proc.Start(); err != nil {
		if errors.Is(err, subprocess.ErrProcessAlreadyStarted) {
			return 0, service.BatchPolicy{}, nil
		}
		return 0, service.BatchPolicy{}, fmt.Errorf("unable to restart plugin: %w", err)
	}
	value, err := runtimepb.AnyToProto(cfgValue)
	if err != nil {
		_ = proc.Close(ctx)
		return 0, service.BatchPolicy{}, fmt.Errorf("unable to convert config to proto: %w", err)
	}
	// Retry to wait for the process to start
	resp, err := backoff.RetryWithData(func() (*runtimepb.BatchOutputInitResponse, error) {
		resp, err := client.Init(ctx, &runtimepb.BatchOutputInitRequest{
			Config: value,
		})
		if err != nil {
			if !proc.IsRunning() {
				return nil, backoff.Permanent(fmt.Errorf("plugin exited early: %w", err))
			}
			return nil, err
		}
		if err = runtimepb.ProtoToError(resp.Error); err != nil {
			return nil, backoff.Permanent(err)
		}
		return resp, nil
	}, backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(maxStartupTime)))
	if err != nil {
		_ = proc.Close(ctx)
		return 0, service.BatchPolicy{}, fmt.Errorf("unable to initialize plugin: %w", err)
	}
	batchPolicy.ByteSize = int(resp.GetBatchPolicy().GetByteSize())
	batchPolicy.Count = int(resp.GetBatchPolicy().GetCount())
	batchPolicy.Period = resp.GetBatchPolicy().GetPeriod()
	batchPolicy.Check = resp.GetBatchPolicy().GetCheck()
	maxInFlight = int(resp.GetMaxInFlight())
	return
}

// Connect implements service.BatchOutput.
func (o *output) Connect(ctx context.Context) (err error) {
	var resp *runtimepb.BatchOutputConnectResponse
	// If the plugin crashes attempt to restart the process up to retryCount times.
	for range retryCount {
		resp, err = o.client.Connect(ctx, &runtimepb.BatchOutputConnectRequest{})
		if err != nil {
			err = fmt.Errorf("unable to reach plugin: %w", err)
			if o.proc.IsRunning() {
				return err
			}
			if err := o.proc.Close(ctx); err != nil {
				return fmt.Errorf("unable to restart plugin process: %w", err)
			}
			if _, _, err := startOutputPlugin(ctx, o.proc, o.client, o.cfgValue); err != nil {
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

// Connect implements service.BatchOutput.
func (o *output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	proto, err := runtimepb.MessageBatchToProto(batch)
	if err != nil {
		return fmt.Errorf("unable to convert batch to proto: %w", err)
	}
	resp, err := o.client.Send(ctx, &runtimepb.BatchOutputSendRequest{
		Batch: proto,
	})
	if err != nil {
		if !o.proc.IsRunning() {
			return service.ErrNotConnected
		}
		return fmt.Errorf("unable to read from plugin: %w", err)
	}
	return runtimepb.ProtoToError(resp.Error)
}

// Close implements service.BatchOutput.
func (o *output) Close(ctx context.Context) error {
	resp, err := o.client.Close(ctx, &runtimepb.BatchOutputCloseRequest{})
	if err != nil {
		return fmt.Errorf("unable to close plugin: %w", err)
	}
	if err := runtimepb.ProtoToError(resp.Error); err != nil {
		return fmt.Errorf("plugin close error: %w", err)
	}
	if err := o.proc.Close(ctx); err != nil {
		return fmt.Errorf("unable to close plugin process: %w", err)
	}
	return nil
}
