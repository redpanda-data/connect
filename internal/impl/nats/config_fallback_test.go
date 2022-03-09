package nats_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/old/input"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestConfigFallback(t *testing.T) {
	conf := input.NewConfig()
	conf.Type = input.TypeNATSJetStream
	conf.NATSJetStream.Deliver = "not recognised"

	testMgr, err := manager.NewV2(manager.NewResourceConfig(), nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = input.New(conf, testMgr, log.Noop(), metrics.Noop())
	require.Error(t, err)

	assert.Contains(t, err.Error(), "deliver option not recognised was not recognised")
}
