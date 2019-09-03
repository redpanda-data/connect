// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package output

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/types"
)

var logConfig = log.Config{
	LogLevel: "NONE",
}

// MockOutputType implements the output.Type interface.
type MockOutputType struct {
	TChan <-chan types.Transaction
}

// Consume sets the read channel. This implementation is NOT thread safe.
func (m *MockOutputType) Consume(msgs <-chan types.Transaction) error {
	m.TChan = msgs
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (m *MockOutputType) Connected() bool {
	return true
}

// CloseAsync does nothing.
func (m *MockOutputType) CloseAsync() {
}

// WaitForClose does nothing.
func (m MockOutputType) WaitForClose(t time.Duration) error {
	return nil
}
