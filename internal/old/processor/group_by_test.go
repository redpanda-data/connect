package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestGroupBy(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeGroupBy

	procConf := NewConfig()
	procConf.Type = TypeArchive
	procConf.Archive.Format = "lines"

	conf.GroupBy = append(conf.GroupBy, GroupByElement{
		Check: `content().contains("foo")`,
		Processors: []Config{
			procConf,
		},
	})

	procConf = NewConfig()
	procConf.Type = TypeBloblang
	procConf.Bloblang = `root = content().uppercase()`

	procConf2 := NewConfig()
	procConf2.Type = TypeBloblang
	procConf2.Bloblang = `root = content().trim()`

	conf.GroupBy = append(conf.GroupBy, GroupByElement{
		Check: `content().contains("bar")`,
		Processors: []Config{
			procConf,
			procConf2,
		},
	})

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
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
	msgs, res := proc.ProcessMessage(input)
	require.Nil(t, res)

	for _, msg := range msgs {
		act = append(act, message.GetAllBytes(msg))
	}
	assert.Equal(t, exp, act)
}

func TestGroupByErrs(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeGroupBy

	procConf := NewConfig()
	procConf.Type = TypeArchive
	procConf.Archive.Format = "lines"

	conf.GroupBy = append(conf.GroupBy, GroupByElement{
		Processors: []Config{
			procConf,
		},
	})

	_, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.EqualError(t, err, "a group definition must have a check query")
}
