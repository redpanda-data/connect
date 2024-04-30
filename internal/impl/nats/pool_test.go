package nats

import (
  "context"
  "testing"

  "github.com/nats-io/nats.go"
  "github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
  t.Run("shouldCreateIfNotExists", shouldCreateIfNotExists)
  t.Run("shouldReuseIfExists", shouldReuseIfExists)
  t.Run("shouldReuseIfAskedMultipleTimes", shouldReuseIfAskedMultipleTimes)
}

func shouldCreateIfNotExists(t *testing.T) {
  pl := &connectionPool{
    cache: map[string]*connRef{},
    connectFn: func(ctx context.Context, s string, details connectionDetails) (*nats.Conn, error) {
      return &nats.Conn{Opts: nats.Options{Name: s}}, nil
    },
  }

  pk := "default"
  caller := "caller_id"
  cd := connectionDetails{poolKey: pk, urls: "url1, url2"}

  res, err := pl.Get(context.Background(), caller, cd)
  assert.NoError(t, err)
  assert.NotNil(t, res)
  assert.NotNil(t, pl.cache[pk])
  assert.Equal(t, caller, pl.cache[pk].Nc.Opts.Name)
  assert.Equal(t, caller, pl.cache[pk].References[0])
}

func shouldReuseIfExists(t *testing.T) {
  pl := &connectionPool{
    cache: map[string]*connRef{},
    connectFn: func(ctx context.Context, s string, details connectionDetails) (*nats.Conn, error) {
      return &nats.Conn{Opts: nats.Options{Name: s}}, nil
    },
  }

  c1 := "caller_id_1"
  c2 := "caller_id_2"

  cd := connectionDetails{poolKey: "default", urls: "url1, url2"}
  res1, err := pl.Get(context.Background(), c1, cd)
  assert.NoError(t, err)
  assert.NotNil(t, res1)

  res2, err := pl.Get(context.Background(), c2, cd)
  assert.NoError(t, err)
  assert.NotNil(t, res2)

  assert.Equal(t, []string{c1, c2}, pl.cache[cd.poolKey].References)
  assert.Len(t, pl.cache, 1)

  assert.Equal(t, res1, res2)
}

func shouldReuseIfAskedMultipleTimes(t *testing.T) {
  pl := &connectionPool{
    cache: map[string]*connRef{},
    connectFn: func(ctx context.Context, s string, details connectionDetails) (*nats.Conn, error) {
      return &nats.Conn{Opts: nats.Options{Name: s}}, nil
    },
  }

  c1 := "caller_id_1"

  cd := connectionDetails{poolKey: "default", urls: "url1, url2"}
  res1, err := pl.Get(context.Background(), c1, cd)
  assert.NoError(t, err)
  assert.NotNil(t, res1)

  res2, err := pl.Get(context.Background(), c1, cd)
  assert.NoError(t, err)
  assert.NotNil(t, res2)

  assert.Equal(t, []string{c1}, pl.cache[cd.poolKey].References)
  assert.Len(t, pl.cache, 1)

  assert.Equal(t, res1, res2)
}
