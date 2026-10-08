// Copyright 2026 Redpanda Data, Inc.
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

package cohere

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// The penalty fields follow the Cohere API, which accepts 0 to 1, not the
// -2 to 2 range of the OpenAI API they were copied from.
func TestChatProcessorPenaltyLintRange(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	for _, field := range []string{"frequency_penalty", "presence_penalty"} {
		for _, tt := range []struct {
			value   string
			wantErr bool
		}{
			{"0", false},
			{"0.5", false},
			{"1", false},
			{"-0.1", true},
			{"1.1", true},
			{"-2", true},
			{"2", true},
		} {
			t.Run(field+"="+tt.value, func(t *testing.T) {
				conf := "cohere_chat:\n  api_key: x\n  model: command-r\n  " + field + ": " + tt.value + "\n"
				lints, err := linter.LintProcessorYAML([]byte(conf))
				require.NoError(t, err)
				var msgs []string
				for _, l := range lints {
					msgs = append(msgs, l.Error())
				}
				if !tt.wantErr {
					assert.Empty(t, msgs)
					return
				}
				assert.Contains(t, strings.Join(msgs, "\n"), "field must be between 0 and 1")
			})
		}
	}
}
