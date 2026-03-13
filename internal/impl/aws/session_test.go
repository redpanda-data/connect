package aws

import (
	"context"
	"os"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSessionWithProvidedBundle(t *testing.T) {
	println(os.Getwd())
	t.Setenv("AWS_CA_BUNDLE", "./resources/dummy_ca.crt")
	conf := `
tcp:
  connect_timeout: 2s
`
	spec := service.NewConfigSpec().
		Categories("Services", "AWS").
		Fields(config.SessionFields()...)
	pConf, err := spec.ParseYAML(conf, nil)
	require.NoError(t, err)
	_, err = GetSession(context.TODO(), pConf)
	assert.NoError(t, err)
}
