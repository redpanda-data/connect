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

package util

import (
	"sort"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// ClosablePool keeps a reference to a pool of closable types and closes them in
// tiers.
type ClosablePool struct {
	closables map[int][]types.Closable
}

// NewClosablePool creates a fresh pool of closable types.
func NewClosablePool() *ClosablePool {
	return &ClosablePool{
		closables: make(map[int][]types.Closable),
	}
}

//------------------------------------------------------------------------------

// Add adds a closable type to the pool, tiers are used to partition and order
// the closing of types (starting at the lowest tier and working upwards).
// Closable types in a single tier are closed in the order that they are added.
func (c *ClosablePool) Add(tier int, closable types.Closable) {
	tierArray := []types.Closable{}
	if t, ok := c.closables[tier]; ok {
		tierArray = t
	}
	c.closables[tier] = append(tierArray, closable)
}

// Close attempts to close and clean up all stored closables in the determined
// order. If the timeout is met whilst working through the pool there is no
// indication of how far the pool has been progressed, thus this timeout should
// only be used for preventing severe blocking of a service.
func (c *ClosablePool) Close(timeout time.Duration) error {
	started := time.Now()

	tiers := []int{}
	for i := range c.closables {
		tiers = append(tiers, i)
	}
	sort.Ints(tiers)

	for _, i := range tiers {
		tier := c.closables[i]
		for j := range tier {
			tier[j].CloseAsync()
		}
		for j := range tier {
			if err := tier[j].WaitForClose(timeout - time.Since(started)); err != nil {
				return err
			}
		}
		delete(c.closables, i)
	}
	return nil
}

//------------------------------------------------------------------------------
