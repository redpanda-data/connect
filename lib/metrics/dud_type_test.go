package metrics

import "testing"

func TestDudInterface(t *testing.T) {
	d := DudType{}
	if Type(d) == nil {
		t.Errorf("DudType does not satisfy Type interface.")
	}
}
