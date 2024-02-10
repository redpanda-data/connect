package metrics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"

	_ "github.com/benthosdev/benthos/v4/public/components/prometheus"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestMappingConfigAny(t *testing.T) {
	conf, err := metrics.FromAny(bundle.GlobalEnvironment, map[string]any{
		"prometheus": map[string]any{},
		"mapping":    `meta foo = "bar"`,
	})
	require.NoError(t, err)

	ns, err := bundle.AllMetrics.Init(conf, mock.NewManager())
	require.NoError(t, err)

	ctrTwo := ns.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)
	ctrTwo.With("value3").IncrFloat64(10.452)

	body := getPage(t, ns.Child().HandlerFunc())

	assert.Contains(t, body, "\ncountertwo{foo=\"bar\",label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{foo=\"bar\",label1=\"value2\"} 11")
	assert.Contains(t, body, "\ncountertwo{foo=\"bar\",label1=\"value3\"} 10.452")
}

func TestMappingConfigYAML(t *testing.T) {
	n, err := docs.UnmarshalYAML([]byte(`
prometheus: {}
mapping: 'meta foo = "bar"'
`))
	require.NoError(t, err)

	conf, err := metrics.FromAny(bundle.GlobalEnvironment, n)
	require.NoError(t, err)

	ns, err := bundle.AllMetrics.Init(conf, mock.NewManager())
	require.NoError(t, err)

	ctrTwo := ns.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)
	ctrTwo.With("value3").IncrFloat64(10.452)

	body := getPage(t, ns.Child().HandlerFunc())

	assert.Contains(t, body, "\ncountertwo{foo=\"bar\",label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{foo=\"bar\",label1=\"value2\"} 11")
	assert.Contains(t, body, "\ncountertwo{foo=\"bar\",label1=\"value3\"} 10.452")
}
