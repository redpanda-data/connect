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

package types

import (
	"net/http"
)

// DudMgr is a noop implementation of a types.Manager.
type DudMgr struct {
	ID int
}

// RegisterEndpoint is a noop.
func (f DudMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}

// GetCache always returns ErrCacheNotFound.
func (f DudMgr) GetCache(name string) (Cache, error) {
	return nil, ErrCacheNotFound
}

// GetCondition always returns ErrConditionNotFound.
func (f DudMgr) GetCondition(name string) (Condition, error) {
	return nil, ErrConditionNotFound
}

// GetPipe attempts to find a service wide message producer by its name.
func (f DudMgr) GetPipe(name string) (<-chan Transaction, error) {
	return nil, ErrPipeNotFound
}

// SetPipe registers a message producer under a name.
func (f DudMgr) SetPipe(name string, t <-chan Transaction) {}

// UnsetPipe removes a named pipe.
func (f DudMgr) UnsetPipe(name string, t <-chan Transaction) {}

// NoopMgr returns a Manager implementation that does nothing.
func NoopMgr() Manager {
	return DudMgr{}
}
