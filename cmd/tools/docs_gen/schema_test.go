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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func TestComponentExamples(t *testing.T) {
	sch := schema.Standard("", "")
	env := sch.Environment()

	linter := sch.NewStreamConfigLinter()
	linter.SetRejectDeprecated(true)
	linter.SetSkipEnvVarCheck(true)

	testComponent := func(name string, config *service.ConfigView) {
		data, err := config.TemplateData()
		require.NoError(t, err, name)

		t.Run(data.Type+":"+name, func(t *testing.T) {
			for _, e := range data.Examples {
				lints, err := linter.LintYAML([]byte(e.Config))
				require.NoError(t, err)
				for _, l := range lints {
					t.Error(l.Error())
				}
			}
		})
	}

	env.WalkBuffers(testComponent)
	env.WalkCaches(testComponent)
	env.WalkInputs(testComponent)
	env.WalkMetrics(testComponent)
	env.WalkOutputs(testComponent)
	env.WalkProcessors(testComponent)
	env.WalkRateLimits(testComponent)
	env.WalkScanners(testComponent)
	env.WalkTracers(testComponent)
}
