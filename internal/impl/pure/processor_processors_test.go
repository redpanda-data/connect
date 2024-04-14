package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestProcessors(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
processors:
  - bloblang: 'root = content().uppercase()'
  - bloblang: 'root = content().trim()'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	exp := [][][]byte{
		{
			[]byte(`HELLO FOO WORLD 1`),
			[]byte(`HELLO WORLD 1`),
			[]byte(`HELLO BAR WORLD 2`),
		},
	}
	act := [][][]byte{}

	input := message.QuickBatch([][]byte{
		[]byte(` hello foo world 1 `),
		[]byte(` hello world 1 `),
		[]byte(` hello bar world 2 `),
	})
	msgs, res := proc.ProcessBatch(context.Background(), input)
	require.NoError(t, res)

	for _, msg := range msgs {
		act = append(act, message.GetAllBytes(msg))
	}
	assert.Equal(t, exp, act)
}
