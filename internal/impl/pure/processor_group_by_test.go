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

func TestGroupBy(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
group_by:
  - check: 'content().contains("foo")'
    processors:
      - archive:
          format: lines
  - check: 'content().contains("bar")'
    processors:
      - bloblang: 'root = content().uppercase()'
      - bloblang: 'root = content().trim()'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	exp := [][][]byte{
		{
			[]byte(` hello foo world 1 
 hello foo bar world 1 
 hello foo world 2 
 hello foo bar world 2 `),
		},
		{
			[]byte(`HELLO BAR WORLD 1`),
			[]byte(`HELLO BAR WORLD 2`),
		},
		{
			[]byte(` hello world 1 `),
			[]byte(` hello world 2 `),
		},
	}
	act := [][][]byte{}

	input := message.QuickBatch([][]byte{
		[]byte(` hello foo world 1 `),
		[]byte(` hello world 1 `),
		[]byte(` hello foo bar world 1 `),
		[]byte(` hello bar world 1 `),
		[]byte(` hello foo world 2 `),
		[]byte(` hello world 2 `),
		[]byte(` hello foo bar world 2 `),
		[]byte(` hello bar world 2 `),
	})
	msgs, res := proc.ProcessBatch(context.Background(), input)
	require.NoError(t, res)

	for _, msg := range msgs {
		act = append(act, message.GetAllBytes(msg))
	}
	assert.Equal(t, exp, act)
}

func TestGroupByErrs(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
group_by:
  - processors:
      - archive:
          format: lines
`)
	require.NoError(t, err)

	_, err = mock.NewManager().NewProcessor(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "field 'check' is required")
}
