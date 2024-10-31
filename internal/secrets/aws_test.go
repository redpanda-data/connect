package secrets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getRegion(t *testing.T) {
	type args struct {
		host string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should accept region as endpoint",
			args: args{
				host: "eu-west-1",
			},
			want: "eu-west-1",
		},
		{
			name: "should get region from regional AWS endpoint",
			args: args{
				host: "eu-west-1.amazonaws.com",
			},
			want: "eu-west-1",
		},
		{
			name: "should get region from Secrets Manager endpoint",
			args: args{
				host: "secretsmanager.eu-west-1.amazonaws.com",
			},
			want: "eu-west-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, *getRegion(tt.args.host), "getRegion(%v)", tt.args.host)
		})
	}
}
