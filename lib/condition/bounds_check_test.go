package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestBoundsCheck(t *testing.T) {
	type fields struct {
		maxParts    int
		maxPartSize int
		minParts    int
		minPartSize int
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "bounds_check pos 1",
			fields: fields{
				maxParts:    5,
				maxPartSize: 1.073741824e+09,
				minParts:    1,
				minPartSize: 1,
			},
			arg: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
			want: true,
		},
		{
			name: "bounds_check maxParts neg 1",
			fields: fields{
				maxParts:    1,
				maxPartSize: 1.073741824e+09,
				minParts:    1,
				minPartSize: 1,
			},
			arg: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
			want: false,
		},
		{
			name: "bounds_check minParts neg 1",
			fields: fields{
				maxParts:    5,
				maxPartSize: 1.073741824e+09,
				minParts:    3,
				minPartSize: 1,
			},
			arg: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
			want: false,
		},
		{
			name: "bounds_check minParts neg 2",
			fields: fields{
				maxParts:    5,
				maxPartSize: 1.073741824e+09,
				minParts:    1,
				minPartSize: 1,
			},
			arg:  [][]byte{},
			want: false,
		},
		{
			name: "bounds_check maxPartSize neg 1",
			fields: fields{
				maxParts:    5,
				maxPartSize: 3,
				minParts:    1,
				minPartSize: 1,
			},
			arg: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
			want: false,
		},
		{
			name: "bounds_check minPartSize neg 1",
			fields: fields{
				maxParts:    5,
				maxPartSize: 1.073741824e+09,
				minParts:    1,
				minPartSize: 10,
			},
			arg: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = TypeBoundsCheck
			conf.BoundsCheck.MaxParts = tt.fields.maxParts
			conf.BoundsCheck.MaxPartSize = tt.fields.maxPartSize
			conf.BoundsCheck.MinParts = tt.fields.minParts
			conf.BoundsCheck.MinPartSize = tt.fields.minPartSize

			c, err := NewBoundsCheck(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Fatal(err)
			}
			msg := message.New(tt.arg)
			if got := c.Check(msg); got != tt.want {
				t.Errorf("BoundsCheck.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}
