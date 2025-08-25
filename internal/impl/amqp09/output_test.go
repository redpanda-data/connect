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

package amqp09

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestAMQP09WriterFromParsedWithExchangeDeclareArgs(t *testing.T) {
	// This test reproduces issue #3576 - panic when exchangeDeclareArgs is nil
	// but we try to assign to it when exchange_declare.arguments is configured
	
	configYAML := `
urls:
  - amqp://guest:guest@localhost:5672/
exchange: test-exchange
exchange_declare:
  enabled: true
  type: direct
  durable: true
  arguments:
    alternate-exchange: my-ae
    x-message-ttl: 60000
`

	spec := amqp09OutputSpec()
	config, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	// This should not panic
	writer, err := amqp09WriterFromParsed(config, service.MockResources())
	require.NoError(t, err)
	require.NotNil(t, writer)
	
	// Verify that the arguments were properly set
	require.NotNil(t, writer.exchangeDeclareArgs)
	require.Equal(t, "my-ae", writer.exchangeDeclareArgs["alternate-exchange"])
	require.Equal(t, "60000", writer.exchangeDeclareArgs["x-message-ttl"])
}

func TestAMQP09WriterFromParsedWithoutExchangeDeclareArgs(t *testing.T) {
	// Test that configuration without arguments still works
	
	configYAML := `
urls:
  - amqp://guest:guest@localhost:5672/
exchange: test-exchange
exchange_declare:
  enabled: true
  type: direct
  durable: true
`

	spec := amqp09OutputSpec()
	config, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	writer, err := amqp09WriterFromParsed(config, service.MockResources())
	require.NoError(t, err)
	require.NotNil(t, writer)
	
	// exchangeDeclareArgs should remain nil when no arguments are configured
	require.Nil(t, writer.exchangeDeclareArgs)
}