package io_test

import (
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

	s.CloseAsync()
	require.NoError(t, s.WaitForClose(time.Second))
}
