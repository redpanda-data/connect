package pure_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestTryEmpty(t *testing.T) {
	conf, err := processor.FromYAML(`
try: []
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte("foo bar baz"),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(exp))
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryBasic(t *testing.T) {
	conf, err := processor.FromYAML(`
try:
  - bloblang: 'root = if batch_index() == 0 { content().encode("base64") }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("MSAyIDMgNA=="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryFilterSome(t *testing.T) {
	conf, err := processor.FromYAML(`
try:
  - bloblang: 'root = if !content().contains("foo") { deleted() }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("foo bar baz"),
		[]byte("hello foo world"),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryMultiProcs(t *testing.T) {
	conf, err := processor.FromYAML(`
try:
  - bloblang: 'root = if !content().contains("foo") { deleted() }'
  - bloblang: 'root = if batch_index() == 0 { content().encode("base64") }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("foo bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello foo world"),
	}
	exp := [][]byte{
		[]byte("Zm9vIGJhciBiYXo="),
		[]byte("aGVsbG8gZm9vIHdvcmxk"),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
}

func TestTryFailJSON(t *testing.T) {
	conf, err := processor.FromYAML(`
try:
  - jmespath:
      query: 'foo'
  - bloblang: 'root = if batch_index() == 0 { content().encode("base64") }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte(`{"foo":{"bar":"baz"}}`),
		[]byte("NOT VALID JSON"),
		[]byte(`{"foo":{"bar":"baz2"}}`),
	}
	exp := [][]byte{
		[]byte("eyJiYXIiOiJiYXoifQ=="),
		[]byte("NOT VALID JSON"),
		[]byte("eyJiYXIiOiJiYXoyIn0="),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
	if res != nil {
		t.Fatal(res)
	}

	if len(msgs) != 1 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong results: %s != %s", act, exp)
	}
	if msgs[0].Get(0).ErrorGet() != nil {
		t.Error("Unexpected part 0 failed flag")
	}
	if msgs[0].Get(1).ErrorGet() == nil {
		t.Error("Unexpected part 1 failed flag")
	}
	if msgs[0].Get(2).ErrorGet() != nil {
		t.Error("Unexpected part 2 failed flag")
	}
}

func TestTryFilterAll(t *testing.T) {
	conf, err := processor.FromYAML(`
try:
  - bloblang: 'root = if !content().contains("foo") { deleted() }'
`)
	require.NoError(t, err)

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{
		[]byte("bar baz"),
		[]byte("1 2 3 4"),
		[]byte("hello world"),
	}
	msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
	assert.NoError(t, res)
	if len(msgs) != 0 {
		t.Errorf("Wrong count of result msgs: %v", len(msgs))
	}
}
