package pure_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestSwitchCases(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
switch:
  - check: 'content().contains("A")'
    processors:
      - bloblang: 'root = "Hit case 0: " + content().string()'
  - check: 'content().contains("B")'
    processors:
      - bloblang: 'root = "Hit case 1: " + content().string()'
    fallthrough: true
  - check: 'content().contains("C")'
    processors:
      - bloblang: 'root = "Hit case 2: " + content().string()'
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()
		assert.NoError(t, c.Close(ctx))
	}()

	type testCase struct {
		name     string
		input    []string
		expected []string
	}
	tests := []testCase{
		{
			name:  "switch test 1",
			input: []string{"A", "AB"},
			expected: []string{
				"Hit case 0: A",
				"Hit case 0: AB",
			},
		},
		{
			name:  "switch test 2",
			input: []string{"B", "BC"},
			expected: []string{
				"Hit case 2: Hit case 1: B",
				"Hit case 2: Hit case 1: BC",
			},
		},
		{
			name:  "switch test 3",
			input: []string{"C", "CD"},
			expected: []string{
				"Hit case 2: C",
				"Hit case 2: CD",
			},
		},
		{
			name:  "switch test 4",
			input: []string{"A", "B", "C"},
			expected: []string{
				"Hit case 0: A",
				"Hit case 2: Hit case 1: B",
				"Hit case 2: C",
			},
		},
		{
			name:     "switch test 5",
			input:    []string{"D"},
			expected: []string{"D"},
		},
		{
			name:  "switch test 6",
			input: []string{"B", "C", "A"},
			expected: []string{
				"Hit case 2: Hit case 1: B",
				"Hit case 2: C",
				"Hit case 0: A",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			msg := message.QuickBatch(nil)
			for _, s := range test.input {
				msg = append(msg, message.NewPart([]byte(s)))
			}
			msgs, res := c.ProcessBatch(context.Background(), msg)
			require.NoError(t, res)

			resStrs := []string{}
			for _, b := range message.GetAllBytes(msgs[0]) {
				resStrs = append(resStrs, string(b))
			}
			assert.Equal(t, test.expected, resStrs)
		})
	}
}

func TestSwitchError(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
switch:
  - check: 'this.id.not_empty().contains("foo")'
    processors:
      - bloblang: 'root = "Hit case 0: " + content().string()'
  - check: 'this.content.contains("bar")'
    processors:
      - bloblang: 'root = "Hit case 1: " + content().string()'
`)
	require.NoError(t, err)

	c, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()
		assert.NoError(t, c.Close(ctx))
	}()

	msg := message.Batch{
		message.NewPart([]byte(`{"id":"foo","content":"just a foo"}`)),
		message.NewPart([]byte(`{"content":"bar but doesnt have an id!"}`)),
		message.NewPart([]byte(`{"id":"buz","content":"a real foobar"}`)),
	}

	msgs, res := c.ProcessBatch(context.Background(), msg)
	require.NoError(t, res)

	assert.Len(t, msgs, 1)
	assert.Equal(t, 3, msgs[0].Len())

	resStrs := []string{}
	for _, b := range message.GetAllBytes(msgs[0]) {
		resStrs = append(resStrs, string(b))
	}

	assert.NoError(t, msgs[0].Get(0).ErrorGet())
	assert.EqualError(t, msgs[0].Get(1).ErrorGet(), "failed assignment (line 1): expected string, array or object value, got null from field `this.id`")
	assert.NoError(t, msgs[0].Get(2).ErrorGet())

	assert.Equal(t, []string{
		`Hit case 0: {"id":"foo","content":"just a foo"}`,
		`{"content":"bar but doesnt have an id!"}`,
		`Hit case 1: {"id":"buz","content":"a real foobar"}`,
	}, resStrs)
}

func BenchmarkSwitch10(b *testing.B) {
	conf, err := testutil.ProcessorFromYAML(`
switch:
  - check: 'content().contains("A")'
    processors:
      - bloblang: 'root = "Hit case 0: " + content().string()'
  - check: 'content().contains("B")'
    processors:
      - bloblang: 'root = "Hit case 1: " + content().string()'
    fallthrough: true
  - check: 'content().contains("C")'
    processors:
      - bloblang: 'root = "Hit case 2: " + content().string()'
`)
	require.NoError(b, err)

	c, err := mock.NewManager().NewProcessor(conf)
	require.NoError(b, err)
	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()
		assert.NoError(b, c.Close(ctx))
	}()

	msg := message.QuickBatch([][]byte{
		[]byte("A"),
		[]byte("B"),
		[]byte("C"),
		[]byte("D"),
		[]byte("AB"),
		[]byte("AC"),
		[]byte("AD"),
		[]byte("BC"),
		[]byte("BD"),
		[]byte("CD"),
	})

	exp := [][]byte{
		[]byte("Hit case 0: A"),
		[]byte("Hit case 2: Hit case 1: B"),
		[]byte("Hit case 2: C"),
		[]byte("D"),
		[]byte("Hit case 0: AB"),
		[]byte("Hit case 0: AC"),
		[]byte("Hit case 0: AD"),
		[]byte("Hit case 2: Hit case 1: BC"),
		[]byte("Hit case 2: Hit case 1: BD"),
		[]byte("Hit case 2: CD"),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msgs, res := c.ProcessBatch(context.Background(), msg)
		require.NoError(b, res)
		assert.Equal(b, exp, message.GetAllBytes(msgs[0]))
	}
}

func BenchmarkSwitch1(b *testing.B) {
	conf, err := testutil.ProcessorFromYAML(`
switch:
  - check: 'content().contains("A")'
    processors:
      - bloblang: 'root = "Hit case 0: " + content().string()'
  - check: 'content().contains("B")'
    processors:
      - bloblang: 'root = "Hit case 1: " + content().string()'
    fallthrough: true
  - check: 'content().contains("C")'
    processors:
      - bloblang: 'root = "Hit case 2: " + content().string()'
`)
	require.NoError(b, err)

	c, err := mock.NewManager().NewProcessor(conf)
	require.NoError(b, err)
	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()
		assert.NoError(b, c.Close(ctx))
	}()

	msgs := []message.Batch{
		message.QuickBatch([][]byte{[]byte("A")}),
		message.QuickBatch([][]byte{[]byte("B")}),
		message.QuickBatch([][]byte{[]byte("C")}),
		message.QuickBatch([][]byte{[]byte("D")}),
		message.QuickBatch([][]byte{[]byte("AB")}),
		message.QuickBatch([][]byte{[]byte("AC")}),
		message.QuickBatch([][]byte{[]byte("AD")}),
		message.QuickBatch([][]byte{[]byte("BC")}),
		message.QuickBatch([][]byte{[]byte("BD")}),
		message.QuickBatch([][]byte{[]byte("CD")}),
	}

	exp := [][]byte{
		[]byte("Hit case 0: A"),
		[]byte("Hit case 2: Hit case 1: B"),
		[]byte("Hit case 2: C"),
		[]byte("D"),
		[]byte("Hit case 0: AB"),
		[]byte("Hit case 0: AC"),
		[]byte("Hit case 0: AD"),
		[]byte("Hit case 2: Hit case 1: BC"),
		[]byte("Hit case 2: Hit case 1: BD"),
		[]byte("Hit case 2: CD"),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resMsgs, res := c.ProcessBatch(context.Background(), msgs[i%len(msgs)])
		require.NoError(b, res)
		assert.Equal(b, [][]byte{exp[i%len(exp)]}, message.GetAllBytes(resMsgs[0]))
	}
}

func BenchmarkSortCorrect(b *testing.B) {
	sortedParts := make([]*message.Part, b.N)
	for i := range sortedParts {
		sortedParts[i] = message.NewPart([]byte(fmt.Sprintf("hello world %040d", i)))
	}

	group, parts := message.NewSortGroup(sortedParts)

	b.ReportAllocs()
	b.ResetTimer()

	pure.SwitchReorderFromGroup(group, parts)
}

func BenchmarkSortReverse(b *testing.B) {
	sortedParts := make([]*message.Part, b.N)
	for i := range sortedParts {
		sortedParts[i] = message.NewPart([]byte(fmt.Sprintf("hello world %040d", i)))
	}

	group, parts := message.NewSortGroup(sortedParts)
	unsortedParts := make([]*message.Part, b.N)
	for i := range parts {
		unsortedParts[i] = parts[len(parts)-i-1]
	}

	b.ReportAllocs()
	b.ResetTimer()

	pure.SwitchReorderFromGroup(group, unsortedParts)
}
