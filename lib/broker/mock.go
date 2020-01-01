package broker

import "time"

//------------------------------------------------------------------------------

// MockType implements the broker.Type interface.
type MockType struct {
}

// CloseAsync does nothing.
func (m MockType) CloseAsync() {
	// Do nothing
}

// WaitForClose does nothing.
func (m MockType) WaitForClose(time.Duration) error {
	// Do nothing
	return nil
}

//------------------------------------------------------------------------------
