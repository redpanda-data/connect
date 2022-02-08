package util

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
)

//------------------------------------------------------------------------------

type closable struct {
	globalCloseCounter *int
	globalWaitCounter  *int

	waitedAt int
	closedAt int

	waitFor time.Duration
}

func (c *closable) CloseAsync() {
	c.waitedAt = *c.globalWaitCounter
	*c.globalWaitCounter++
}

func (c *closable) WaitForClose(tout time.Duration) error {
	if c.waitFor > 0 {
		if tout < c.waitFor {
			<-time.After(tout)
			return component.ErrTimeout
		}
		<-time.After(c.waitFor)
	}
	c.closedAt = *c.globalCloseCounter
	*c.globalCloseCounter++
	return nil
}

//------------------------------------------------------------------------------

func TestClosablePoolOrdering(t *testing.T) {
	n := 100

	closables := []*closable{}

	pool := NewClosablePool()

	closeCount, waitCount := 0, 0

	for i := 0; i < n; i++ {
		closables = append(closables, &closable{
			globalCloseCounter: &closeCount,
			globalWaitCounter:  &waitCount,
		})
	}

	for i := range closables {
		pool.Add(0, closables[i])
	}

	pool.Close(time.Second)

	if waitCount != n {
		t.Errorf("Wrong global wait count: %v != %v", waitCount, n)
	}
	if closeCount != n {
		t.Errorf("Wrong global close count: %v != %v", closeCount, n)
	}

	for i := range closables {
		if actual := closables[i].waitedAt; actual != i {
			t.Errorf("Wrong closable wait index: %v != %v", actual, i)
		}
		if actual := closables[i].closedAt; actual != i {
			t.Errorf("Wrong closable closed index: %v != %v", actual, i)
		}
	}
}

func TestClosablePoolTierOrdering(t *testing.T) {
	n, tiers, closeCount, waitCount := 100, 5, 0, 0

	closables := [][]*closable{}
	m := map[int]struct{}{}

	pool := NewClosablePool()

	for i := 0; i < tiers; i++ {
		tClosables := []*closable{}
		m[i] = struct{}{}
		for j := 0; j < n; j++ {
			tClosables = append(tClosables, &closable{
				globalCloseCounter: &closeCount,
				globalWaitCounter:  &waitCount,
			})
		}
		closables = append(closables, tClosables)
	}

	// Random iteration
	for i := range m {
		tClosables := closables[i]
		for j := range tClosables {
			pool.Add(i, tClosables[j])
		}
	}

	pool.Close(time.Second)

	if waitCount != n*tiers {
		t.Errorf("Wrong global wait count: %v != %v", waitCount, n*tiers)
	}
	if closeCount != n*tiers {
		t.Errorf("Wrong global close count: %v != %v", closeCount, n*tiers)
	}

	for i := range closables {
		for j := range closables[i] {
			if actual := closables[i][j].waitedAt; actual != i*n+j {
				t.Errorf("Wrong closable wait index: %v != %v", actual, i*n+j)
			}
			if actual := closables[i][j].closedAt; actual != i*n+j {
				t.Errorf("Wrong closable closed index: %v != %v", actual, i*n+j)
			}
		}
	}
}

//------------------------------------------------------------------------------
