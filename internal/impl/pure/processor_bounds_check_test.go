package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestBoundsCheck(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "bounds_check"
	conf.BoundsCheck.MinParts = 2
	conf.BoundsCheck.MaxParts = 3
	conf.BoundsCheck.MaxPartSize = 10
	conf.BoundsCheck.MinPartSize = 1

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Error(err)
		return
	}

	goodParts := [][][]byte{
		{
			[]byte("hello"),
			[]byte("world"),
		},
		{
			[]byte("helloworld"),
			[]byte("helloworld"),
		},
		{
			[]byte("hello"),
			[]byte("world"),
			[]byte("!"),
		},
		{
			[]byte("helloworld"),
			[]byte("helloworld"),
			[]byte("helloworld"),
		},
	}

	badParts := [][][]byte{
		{
			[]byte("hello world"),
		},
		{
			[]byte("hello world"),
			[]byte("hello world this exceeds max part size"),
		},
		{
			[]byte("hello"),
			[]byte("world"),
			[]byte("this"),
			[]byte("exceeds"),
			[]byte("max"),
			[]byte("num"),
			[]byte("parts"),
		},
		{
			[]byte("hello"),
			[]byte(""),
		},
	}

	for _, parts := range goodParts {
		msg := message.QuickBatch(parts)
		msgs, _ := proc.ProcessBatch(context.Background(), msg)
		require.Len(t, msgs, 1)
		require.Equal(t, len(parts), msgs[0].Len())
		for i, p := range parts {
			assert.Equal(t, string(p), string(msgs[0].Get(i).AsBytes()), i)
		}
	}

	for _, parts := range badParts {
		msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
		assert.Len(t, msgs, 0)
		assert.Nil(t, res)
	}
}
