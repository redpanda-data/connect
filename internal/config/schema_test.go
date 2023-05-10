package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/cache"
)

func TestConfigClone(t *testing.T) {
	fooCacheConf := cache.NewConfig()
	fooCacheConf.Label = "foocache"

	conf := New()
	conf.Input.Type = "foobar"
	conf.ResourceCaches = append(conf.ResourceCaches, fooCacheConf)

	newConf, err := conf.Clone()
	require.NoError(t, err)

	assert.Equal(t, "foobar", newConf.Input.Type)
	assert.Len(t, newConf.ResourceCaches, 1)

	newConf.Input.Type = "barbaz"

	barCacheConf := cache.NewConfig()
	barCacheConf.Label = "barcache"
	newConf.ResourceCaches = append(newConf.ResourceCaches, barCacheConf)

	assert.Len(t, newConf.ResourceCaches, 2)
	assert.Equal(t, "barbaz", newConf.Input.Type)

	assert.Len(t, conf.ResourceCaches, 1)
	assert.Equal(t, "foobar", conf.Input.Type)
}
