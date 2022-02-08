package component

import "testing"

func TestHTTPError(t *testing.T) {
	err := ErrUnexpectedHTTPRes{
		Code: 0,
		S:    "test str",
		Body: []byte("test body str"),
	}

	exp, act := `HTTP request returned unexpected response code (0): test str, Error: test body str`, err.Error()
	if exp != act {
		t.Errorf("Wrong Error() from ErrUnexpectedHTTPRes: %v != %v", exp, act)
	}
}
