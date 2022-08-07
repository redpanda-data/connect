package io_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestSTDINClose(t *testing.T) {
	conf := input.NewConfig()
	conf.Type = "stdin"
	s, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	s.TriggerStopConsuming()
	require.NoError(t, s.WaitForClose(ctx))
}
