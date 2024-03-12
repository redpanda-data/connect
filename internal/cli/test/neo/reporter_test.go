package neo_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/cli/test/neo"
)

func TestGetFormatter(t *testing.T) {
	type args struct {
		format string
	}
	tests := []struct {
		name    string
		args    args
		want    reflect.Type
		wantErr assert.ErrorAssertionFunc
	}{
		{name: "default", args: args{format: "default"}, want: reflect.TypeOf(neo.DefaultReporter{}), wantErr: assert.NoError},
		{name: "unknown", args: args{format: "unknown"}, want: nil, wantErr: assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBufferString("")
			ew := bytes.NewBufferString("")

			got, err := neo.GetFormatter(tt.args.format, w, ew)
			if !tt.wantErr(t, err, fmt.Sprintf("GetFormatter(%v)", tt.args.format)) {
				return
			}
			assert.Equalf(t, tt.want, reflect.TypeOf(got), "GetFormatter(%v)", tt.args.format)
		})
	}
}
