package serverless_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/serverless"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

func TestServerlessHandlerDefaults(t *testing.T) {
	h, err := serverless.NewHandler(`
pipeline:
  processors:
    - mapping: 'root = content().uppercase()'
logger:
  level: NONE
`)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	res, err := h.Handle(ctx, "hello world")
	require.NoError(t, err)

	assert.Equal(t, "HELLO WORLD", res)

	require.NoError(t, h.Close(ctx))
}
