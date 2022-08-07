package pure_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestInsertBoundaries(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "insert_part"
	conf.InsertPart.Content = "hello world"

	for i := 0; i < 10; i++ {
		for j := -5; j <= 5; j++ {
			conf.InsertPart.Index = j
			proc, err := mock.NewManager().NewProcessor(conf)
			if err != nil {
				t.Error(err)
				return
			}

			var parts [][]byte
			for k := 0; k < i; k++ {
				parts = append(parts, []byte("foo"))
			}

			msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(parts))
			if len(msgs) != 1 {
				t.Error("Insert Part failed")
			} else if res != nil {
				t.Errorf("Expected nil response: %v", res)
			}
			if exp, act := i+1, len(message.GetAllBytes(msgs[0])); exp != act {
				t.Errorf("Wrong count of result parts: %v != %v", act, exp)
			}
		}
	}
}

func TestInsertPart(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "insert_part"
	conf.InsertPart.Content = "hello world"

	type test struct {
		index int
		in    [][]byte
		out   [][]byte
	}

	tests := []test{
		{
			index: 0,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("hello world"),
				[]byte("0"),
				[]byte("1"),
			},
		},
		{
			index: 0,
			in:    [][]byte{},
			out: [][]byte{
				[]byte("hello world"),
			},
		},
		{
			index: 1,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("0"),
				[]byte("hello world"),
				[]byte("1"),
			},
		},
		{
			index: 2,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("hello world"),
			},
		},
		{
			index: 3,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("hello world"),
			},
		},
		{
			index: -1,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("hello world"),
			},
		},
		{
			index: -2,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("0"),
				[]byte("hello world"),
				[]byte("1"),
			},
		},
		{
			index: -3,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("hello world"),
				[]byte("0"),
				[]byte("1"),
			},
		},
		{
			index: -4,
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("hello world"),
				[]byte("0"),
				[]byte("1"),
			},
		},
	}

	for _, test := range tests {
		conf.InsertPart.Index = test.index
		proc, err := mock.NewManager().NewProcessor(conf)
		if err != nil {
			t.Error(err)
			return
		}

		msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(test.in))
		if len(msgs) != 1 {
			t.Errorf("Insert Part failed on: %s", test.in)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := test.out, message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Unexpected output for %s at index %v: %s != %s", test.in, test.index, act, exp)
		}
	}
}

func TestInsertPartInterpolation(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "insert_part"
	conf.InsertPart.Content = "hello ${!hostname()} world"

	hostname, _ := os.Hostname()

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Error(err)
		return
	}

	type test struct {
		in  [][]byte
		out [][]byte
	}

	tests := []test{
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte(fmt.Sprintf("hello %v world", hostname)),
			},
		},
		{
			in: [][]byte{},
			out: [][]byte{
				[]byte(fmt.Sprintf("hello %v world", hostname)),
			},
		},
	}

	for _, test := range tests {
		msgs, res := proc.ProcessBatch(context.Background(), message.QuickBatch(test.in))
		if len(msgs) != 1 {
			t.Errorf("Insert Part failed on: %s", test.in)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := test.out, message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Unexpected output for %s: %s != %s", test.in, act, exp)
		}
	}
}
