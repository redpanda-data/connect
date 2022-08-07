package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestGroupBy(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "group_by"

	procConf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
archive:
  format: lines`), &procConf))

	conf.GroupBy = append(conf.GroupBy, processor.GroupByElement{
		Check: `content().contains("foo")`,
		Processors: []processor.Config{
			procConf,
		},
	})

	procConf = processor.NewConfig()
	procConf.Type = "bloblang"
	procConf.Bloblang = `root = content().uppercase()`

	procConf2 := processor.NewConfig()
	procConf2.Type = "bloblang"
	procConf2.Bloblang = `root = content().trim()`

	conf.GroupBy = append(conf.GroupBy, processor.GroupByElement{
		Check: `content().contains("bar")`,
		Processors: []processor.Config{
			procConf,
			procConf2,
		},
	})

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
	require.Nil(t, res)

	for _, msg := range msgs {
		act = append(act, message.GetAllBytes(msg))
	}
	assert.Equal(t, exp, act)
}

func TestGroupByErrs(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "group_by"

	procConf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
archive:
  format: lines`), &procConf))

	conf.GroupBy = append(conf.GroupBy, processor.GroupByElement{
		Processors: []processor.Config{
			procConf,
		},
	})

	_, err := mock.NewManager().NewProcessor(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "a group definition must have a check query")
}
