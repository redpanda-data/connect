package query_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestMappings(t *testing.T) {
	tests := []struct {
		name         string
		mapping      string
		inputOutputs [][2]string
	}{
		{
			name:    "format_timestamp one nameless arg",
			mapping: `root.something_at = this.created_at.format_timestamp("2006-Jan-02 15:04:05")`,
			inputOutputs: [][2]string{
				{
					`{"created_at":"2020-08-14T11:50:26.371Z"}`,
					`{"something_at":"2020-Aug-14 11:50:26"}`,
				},
			},
		},
		{
			name:    "format_timestamp both nameless args",
			mapping: `root.something_at = this.created_at.format_timestamp("2006-Jan-02 15:04:05", "America/New_York")`,
			inputOutputs: [][2]string{
				{
					`{"created_at":1597405526}`,
					`{"something_at":"2020-Aug-14 07:45:26"}`,
				},
				{
					`{"created_at":"2020-08-14T11:50:26.371Z"}`,
					`{"something_at":"2020-Aug-14 07:50:26"}`,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := bloblang.GlobalEnvironment().NewMapping(test.mapping)
			require.NoError(t, err)

			for i, io := range test.inputOutputs {
				msg := message.QuickBatch([][]byte{[]byte(io[0])})
				p, err := m.MapPart(0, msg)
				exp := io[1]
				if strings.HasPrefix(exp, "Error(") {
					exp = exp[7 : len(exp)-2]
					require.EqualError(t, err, exp, fmt.Sprintf("%v", i))
				} else if exp == "<Message deleted>" {
					require.NoError(t, err)
					require.Nil(t, p)
				} else {
					require.NoError(t, err)
					assert.Equal(t, exp, string(p.AsBytes()), fmt.Sprintf("%v", i))
				}
			}
		})
	}
}
