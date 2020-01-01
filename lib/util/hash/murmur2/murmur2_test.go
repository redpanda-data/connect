package murmur2

import (
	"strconv"
	"testing"
)

func TestMurmur2SanityCheck(t *testing.T) {
	tests := []struct {
		data     []string
		expected int32
	}{
		{[]string{"hello world"}, 1221641059},
		{[]string{"hello" + " " + "world"}, 1221641059},
		// examples from: https://stackoverflow.com/questions/48582589/porting-kafkas-murmur2-implementation-to-go
		{[]string{"21"}, -973932308},
		{[]string{"foobar"}, -790332482},
		{[]string{"a-little-bit-long-string"}, -985981536},
		{[]string{"a-little-bit-longer-string"}, -1486304829},
		{[]string{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"}, -58897971},
		{[]string{"a", "b", "c"}, 479470107},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i)+". ", func(t *testing.T) {
			mur := New32()
			for _, datum := range tt.data {
				_, _ = mur.Write([]byte(datum))
			}
			calculated := mur.Sum32()
			if int32(calculated) != tt.expected {
				t.Errorf("murmur2 hash failed: is -> %v != %v <- should be", calculated, tt.expected)
				return
			}
		})
	}
}
