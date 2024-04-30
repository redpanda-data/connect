package nats

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/nats-io/nats.go"
)

var pool = &connectionPool{
	cache: map[string]*connRef{},
	connectFn: func(ctx context.Context, s string, details connectionDetails) (*nats.Conn, error) {
		return nats.Connect(details.urls, details.opts...)
	},
}

type connectFn func(context.Context, string, connectionDetails) (*nats.Conn, error)

type connectionPool struct {
	lock      sync.Mutex
	cache     map[string]*connRef
	connectFn connectFn
}

type connRef struct {
	Nc         *nats.Conn
	References []string
}

func (c *connectionPool) Get(ctx context.Context, caller string, cd connectionDetails) (*nats.Conn, error) {
	res := c.lookup(caller, cd.poolKey)
	c.lock.Lock()
	defer c.lock.Unlock()
	if res == nil {
		var err error
		if res, err = c.connectFn(ctx, caller, cd); err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}

		c.cache[cd.poolKey] = &connRef{
			Nc:         res,
			References: []string{caller},
		}
	} else {
		if !slices.Contains(c.cache[cd.poolKey].References, caller) {
			c.cache[cd.poolKey].References = append(c.cache[cd.poolKey].References, caller)
		}
	}

	return res, nil
}

func (c *connectionPool) Release(caller string, cd connectionDetails) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	res, fnd := c.cache[cd.poolKey]
	if !fnd {
		return nil
	}

	idx := slices.Index(res.References, caller)
	if idx == -1 {
		return nil
	}

	slices.Delete(res.References, idx, idx+1)

	if len(res.References) == 0 {
		res.Nc.Close()
		delete(c.cache, cd.poolKey)
	}

	return nil
}

func (c *connectionPool) lookup(caller string, key string) *nats.Conn {
	c.lock.Lock()
	defer c.lock.Unlock()

	res, fnd := c.cache[key]
	if !fnd {
		return nil
	}

	if !slices.Contains(res.References, caller) {
		res.References = append(res.References, caller)
	}

	return res.Nc
}
