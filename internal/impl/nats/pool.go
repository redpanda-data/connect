package nats

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/mitchellh/hashstructure"
	"github.com/nats-io/nats.go"
)

var pool = &connectionPool{
	cache: map[uint64]*connRef{},
	connectFn: func(ctx context.Context, s string, details connectionDetails) (*nats.Conn, error) {
		return nats.Connect(s, details.Opts...)
	},
}

type connectFn func(context.Context, string, connectionDetails) (*nats.Conn, error)

type connectionPool struct {
	lock      sync.Mutex
	cache     map[uint64]*connRef
	connectFn connectFn
}

type connRef struct {
	Nc         *nats.Conn
	References []string
}

func (c *connectionPool) Get(ctx context.Context, caller string, cd connectionDetails) (*nats.Conn, error) {
	hash, err := hashstructure.Hash(cd, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to hash connection details: %w", err)
	}

	res := c.lookup(caller, hash)
	c.lock.Lock()
	defer c.lock.Unlock()
	if res == nil {
		var err error
		if res, err = c.connectFn(ctx, caller, cd); err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}

		c.cache[hash] = &connRef{
			Nc:         res,
			References: []string{caller},
		}
	} else {
		if !slices.Contains(c.cache[hash].References, caller) {
			c.cache[hash].References = append(c.cache[hash].References, caller)
		}
	}

	return res, nil
}

func (c *connectionPool) Release(caller string, cd connectionDetails) error {
	hash, err := hashstructure.Hash(cd, nil)
	if err != nil {
		return fmt.Errorf("failed to hash connection details: %w", err)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	res, fnd := c.cache[hash]
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
		delete(c.cache, hash)
	}

	return nil
}

func (c *connectionPool) lookup(caller string, hash uint64) *nats.Conn {
	c.lock.Lock()
	defer c.lock.Unlock()

	res, fnd := c.cache[hash]
	if !fnd {
		return nil
	}

	if !slices.Contains(res.References, caller) {
		res.References = append(res.References, caller)
	}

	return res.Nc
}
