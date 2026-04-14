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

package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestFlagSet() *flag.FlagSet {
	fset := flag.NewFlagSet("test", flag.ContinueOnError)
	fset.Bool("fix", false, "")
	fset.Bool("clean", false, "")
	fset.Bool("debug", false, "")
	fset.Bool("race", false, "")
	fset.String("output-dir", "", "")
	fset.Int("loop", 0, "")
	return fset
}

func TestSplitFlagsAndArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantFlags []string
		wantPosn  []string
	}{
		{
			name:      "flags before positional",
			args:      []string{"--fix", "--clean", "amqp1"},
			wantFlags: []string{"--fix", "--clean"},
			wantPosn:  []string{"amqp1"},
		},
		{
			name:      "interspersed flags and positional",
			args:      []string{"--fix", "amqp1", "--debug", "--race"},
			wantFlags: []string{"--fix", "--debug", "--race"},
			wantPosn:  []string{"amqp1"},
		},
		{
			name:      "value flag with equals",
			args:      []string{"--output-dir=/tmp/out", "kafka"},
			wantFlags: []string{"--output-dir=/tmp/out"},
			wantPosn:  []string{"kafka"},
		},
		{
			name:      "value flag with space",
			args:      []string{"--output-dir", "/tmp/out", "kafka"},
			wantFlags: []string{"--output-dir", "/tmp/out"},
			wantPosn:  []string{"kafka"},
		},
		{
			name:      "int value flag with space interspersed",
			args:      []string{"--fix", "kafka", "--loop", "3", "--debug"},
			wantFlags: []string{"--fix", "--loop", "3", "--debug"},
			wantPosn:  []string{"kafka"},
		},
		{
			name:      "multiple positional args",
			args:      []string{"--fix", "kafka", "redis", "--debug"},
			wantFlags: []string{"--fix", "--debug"},
			wantPosn:  []string{"kafka", "redis"},
		},
		{
			name:     "all positional",
			args:     []string{"kafka", "redis"},
			wantPosn: []string{"kafka", "redis"},
		},
		{
			name:      "all flags",
			args:      []string{"--fix", "--debug", "--race"},
			wantFlags: []string{"--fix", "--debug", "--race"},
		},
		{
			name: "empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags, posn := splitFlagsAndArgs(newTestFlagSet(), tt.args)
			assert.Equal(t, tt.wantFlags, flags)
			assert.Equal(t, tt.wantPosn, posn)
		})
	}
}
