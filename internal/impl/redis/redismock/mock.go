package redismock

import "github.com/benthosdev/benthos/v4/internal/impl/redis"

var (
	_ redis.RedisMinimalInterface             = (*RedisMinimalInterface)(nil)
	_ redis.RedisBloomFilterMinimalInterface  = (*RedisBloomFilterMinimalInterface)(nil)
	_ redis.RedisCuckooFilterMinimalInterface = (*RedisCuckooFilterMinimalInterface)(nil)
)

// Type ''go generate'' to build mock using mockery
//go:generate go install -v github.com/vektra/mockery/v2/...@latest
//go:generate mockery --dir=.. --name=RedisMinimalInterface --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisBloomFilterMinimalInterface --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisCuckooFilterMinimalInterface --case snake --outpkg redismock --output .
