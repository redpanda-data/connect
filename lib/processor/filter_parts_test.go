package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
)

func TestFilterPartsTextCheck(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	tests := []struct {
		name string
		arg  [][]byte
		want [][]byte
	}{
		{
			name: "single part 1",
			arg: [][]byte{
				[]byte("foo"),
			},
			want: nil,
		},
		{
			name: "single part 2",
			arg: [][]byte{
				[]byte("bar"),
			},
			want: [][]byte{
				[]byte("bar"),
			},
		},
		{
			name: "multi part 1",
			arg: [][]byte{
				[]byte("foo"),
				[]byte("foo"),
				[]byte("foo"),
				[]byte("foo"),
			},
			want: nil,
		},
		{
			name: "multi part 2",
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
				[]byte("baz"),
				[]byte("foo"),
			},
			want: [][]byte{
				[]byte("bar"),
				[]byte("baz"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "filter_parts"
			conf.FilterParts.Type = "text"
			conf.FilterParts.Text.Operator = "prefix"
			conf.FilterParts.Text.Part = 0
			conf.FilterParts.Text.Arg = "b"

			c, err := New(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			got, res := c.ProcessMessage(message.New(tt.arg))
			if tt.want == nil {
				if !reflect.DeepEqual(res, response.NewAck()) {
					t.Error("Filter.ProcessMessage() expected drop")
				}
			} else if !reflect.DeepEqual(message.GetAllBytes(got[0]), tt.want) {
				t.Errorf("Filter.ProcessMessage() = %s, want %s", message.GetAllBytes(got[0]), tt.want)
			}
		})
	}
}
