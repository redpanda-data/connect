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

package rpcplugin_test

import (
	"os"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

func TestProcessorSerial(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping test in CI")
	}

	require.NoError(t, rpcplugin.DiscoverAndRegisterPlugins(service.OSFS(), service.GlobalEnvironment(), []string{"./testdata/catshout/plugin.yaml"}))

	resBuilder := service.NewResourceBuilder()
	require.NoError(t, resBuilder.AddProcessorYAML(`
label: foo
catshout:
  suffix: ", and then they lived happily ever after."
`))

	res, done, err := resBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, res.AccessProcessor(t.Context(), "foo", func(proc *service.ResourceProcessor) {
		b, err := proc.Process(t.Context(), service.NewMessage([]byte("hello world")))
		require.NoError(t, err)
		require.Len(t, b, 1)

		bBytes, err := b[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, "MEOW! HELLO WORLD, and then they lived happily ever after.", string(bBytes))
	}))

	require.NoError(t, done(t.Context()))
}

func TestProcessorCustomCwd(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping test in CI")
	}

	require.NoError(t, rpcplugin.DiscoverAndRegisterPlugins(service.OSFS(), service.GlobalEnvironment(), []string{"./testdata/catshout/plugin.custom_dir.yaml"}))

	resBuilder := service.NewResourceBuilder()
	require.NoError(t, resBuilder.AddProcessorYAML(`
label: foo
catshout: {}
`))

	res, done, err := resBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, res.AccessProcessor(t.Context(), "foo", func(proc *service.ResourceProcessor) {
		b, err := proc.Process(t.Context(), service.NewMessage([]byte("hello world")))
		require.NoError(t, err)
		require.Len(t, b, 1)

		bBytes, err := b[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, "MEOW! HELLO WORLD, eh?", string(bBytes))
	}))

	require.NoError(t, done(t.Context()))
}
