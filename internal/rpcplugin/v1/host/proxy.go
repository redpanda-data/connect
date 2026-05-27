// Copyright 2026 Redpanda Data, Inc.
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

package host

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"

	runtimev1 "github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"
	runtimepb "github.com/redpanda-data/connect/v4/internal/rpcplugin/v1/runtimepb"
)

// remoteBatchProcessor implements service.BatchProcessor by routing
// every call to the plugin subprocess via gRPC. Each
// remoteBatchProcessor corresponds to one OpenInstance call against
// the plugin runtime.
type remoteBatchProcessor struct {
	client     runtimepb.PluginRuntimeClient
	instanceID uint64

	componentName string
}

// newRemoteBatchProcessor opens a new processor instance on the
// supplied client.
func newRemoteBatchProcessor(
	ctx context.Context,
	client runtimepb.PluginRuntimeClient,
	componentName string,
	config any,
) (*remoteBatchProcessor, error) {
	configPB, err := runtimev1.AnyToProto(config)
	if err != nil {
		return nil, fmt.Errorf("encoding config: %w", err)
	}
	resp, err := client.OpenInstance(ctx, &runtimepb.OpenInstanceRequest{
		ComponentName: componentName,
		Config:        configPB,
	})
	if err != nil {
		return nil, fmt.Errorf("OpenInstance %q: %w", componentName, err)
	}
	if pbErr := resp.GetError(); pbErr != nil {
		return nil, fmt.Errorf("OpenInstance %q: %w", componentName, runtimev1.ProtoToError(pbErr))
	}
	return &remoteBatchProcessor{
		client:        client,
		instanceID:    resp.GetInstanceId(),
		componentName: componentName,
	}, nil
}

// ProcessBatch forwards the batch to the plugin and decodes the
// response.
func (p *remoteBatchProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batchPB, err := runtimev1.MessageBatchToProto(batch)
	if err != nil {
		return nil, fmt.Errorf("encoding batch: %w", err)
	}
	resp, err := p.client.ProcessBatch(ctx, &runtimepb.InstanceProcessRequest{
		InstanceId: p.instanceID,
		Batch:      batchPB,
	})
	if err != nil {
		return nil, fmt.Errorf("ProcessBatch %q: %w", p.componentName, err)
	}
	if pbErr := resp.GetError(); pbErr != nil {
		return nil, runtimev1.ProtoToError(pbErr)
	}
	out := make([]service.MessageBatch, 0, len(resp.GetBatches()))
	for _, b := range resp.GetBatches() {
		mb, err := runtimev1.ProtoToMessageBatch(b)
		if err != nil {
			return nil, fmt.Errorf("decoding response batch: %w", err)
		}
		out = append(out, mb)
	}
	return out, nil
}

// Close releases the remote instance.
func (p *remoteBatchProcessor) Close(ctx context.Context) error {
	if p.client == nil {
		return nil
	}
	resp, err := p.client.CloseInstance(ctx, &runtimepb.CloseInstanceRequest{
		InstanceId: p.instanceID,
	})
	if err != nil {
		return fmt.Errorf("CloseInstance %q: %w", p.componentName, err)
	}
	if pbErr := resp.GetError(); pbErr != nil {
		return runtimev1.ProtoToError(pbErr)
	}
	return nil
}
