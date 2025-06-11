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

package sideeffect

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterBatchProcessor("side_effect", sideEffectProcessorSpec(), newSideEffectProccessor)
}

const (
	pFieldOutput = "output"
)

func sideEffectProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description(`Write to an output as part of a pipeline step`).
		Fields(
			service.NewOutputField(pFieldOutput).Description("The output to write to"),
		)
}

func newSideEffectProccessor(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
	output, err := conf.FieldOutput(pFieldOutput)
	if err != nil {
		return nil, err
	}
	return &sideEffectProcessor{output}, nil
}

type sideEffectProcessor struct {
	output *service.OwnedOutput
}

var _ service.BatchProcessor = (*sideEffectProcessor)(nil)

// ProcessBatch implements service.BatchProcessor.
func (s *sideEffectProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	err := s.output.WriteBatch(ctx, batch)
	return []service.MessageBatch{batch}, err
}

// Close implements service.BatchProcessor.
func (s *sideEffectProcessor) Close(ctx context.Context) error {
	return s.output.Close(ctx)
}
