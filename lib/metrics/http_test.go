package metrics

import "testing"

func TestHTTPInterface(t *testing.T) {
	o := &HTTP{}
	if Type(o) == nil {
		t.Errorf("Type does not satisfy Type interface.")
	}
}
