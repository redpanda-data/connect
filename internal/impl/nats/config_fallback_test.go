package nats_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
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
