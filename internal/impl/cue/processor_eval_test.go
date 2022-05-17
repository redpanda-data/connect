package cue

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	cueerrors "cuelang.org/go/cue/errors"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestCUEEvalProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := filepath.Abs(filepath.Join(".", "fixtures"))
	require.NoError(t, err)

	spec := newCUEEvalProcessorConfig()
	parsed, err := spec.ParseYAML(fmt.Sprintf(`
dir: %q
package: simple
fill_path: product
selector: test.result
  `, dir), nil)
	require.NoError(t, err)

	proc, err := newCUEEvalProcessor(parsed, &cueEvalProcessorOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(ctx))
	})

	input := service.MessageBatch{
		service.NewMessage([]byte(`{"name": "Cheese Burger", "price": 3.0}`)),
		service.NewMessage([]byte(`{"name": "Salad", "price": 4.1}`)),
	}

	batches, err := proc.ProcessBatch(ctx, input)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	batch := batches[0]
	require.Len(t, batch, len(input))

	msg := batch[0]
	require.NoError(t, msg.GetError())
	actual, err := msg.AsBytes()
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"name": "Cheese Burger", "price": 3, "slug": "cheese-burger"}`,
		string(actual))

	msg = batch[1]
	require.NoError(t, msg.GetError())
	actual, err = msg.AsBytes()
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"name": "Salad", "price": 4.1, "slug": "salad"}`,
		string(actual))
}

func TestCUEEvalProcessor_CUEMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := filepath.Abs(filepath.Join(".", "fixtures"))
	require.NoError(t, err)

	spec := newCUEEvalProcessorConfig()
	parsed, err := spec.ParseYAML(fmt.Sprintf(`
dir: %q
package: simple
fill_path: product
selector: test.result
  `, dir), nil)
	require.NoError(t, err)

	proc, err := newCUEEvalProcessor(parsed, &cueEvalProcessorOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(ctx))
	})

	input := service.MessageBatch{
		service.NewMessage([]byte("name: \"Cheese Burger\"\nprice: 3.0")),
		service.NewMessage([]byte("name: \"Salad\"\nprice: 4.1")),
	}

	batches, err := proc.ProcessBatch(ctx, input)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	batch := batches[0]
	require.Len(t, batch, len(input))

	msg := batch[0]
	require.NoError(t, msg.GetError())
	actual, err := msg.AsBytes()
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"name": "Cheese Burger", "price": 3, "slug": "cheese-burger"}`,
		string(actual))

	msg = batch[1]
	require.NoError(t, msg.GetError())
	actual, err = msg.AsBytes()
	require.NoError(t, err)
	require.JSONEq(
		t,
		`{"name": "Salad", "price": 4.1, "slug": "salad"}`,
		string(actual))
}

func TestCUEEvalProcessor_ValidationError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := filepath.Abs(filepath.Join(".", "fixtures"))
	require.NoError(t, err)

	spec := newCUEEvalProcessorConfig()
	parsed, err := spec.ParseYAML(fmt.Sprintf(`
dir: %q
package: simple
fill_path: product
selector: test.result
  `, dir), nil)
	require.NoError(t, err)

	proc, err := newCUEEvalProcessor(parsed, &cueEvalProcessorOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(ctx))
	})

	input := service.MessageBatch{
		service.NewMessage([]byte(`{"name": "Cheese Burger", "price": "bad value!"}`)),
		service.NewMessage([]byte(`{"name": "Salad", "price": -1}`)),
	}

	batches, err := proc.ProcessBatch(ctx, input)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	batch := batches[0]
	require.Len(t, batch, len(input))

	for _, msg := range batch {
		err = msg.GetError()
		require.Error(t, err)

		var realErr cueerrors.Error
		require.True(t, errors.As(err, &realErr))
		p := realErr.Path()
		require.Equal(t, p, []string{"product", "price"})
	}
}
