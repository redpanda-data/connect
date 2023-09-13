package redismock

// import "github.com/benthosdev/benthos/v4/internal/impl/redis"

// var (
// 	_ redis.RedisCRUD         = (*RedisCRUD)(nil)
// 	_ redis.RedisBloomFilter  = (*RedisBloomFilter)(nil)
// 	_ redis.RedisCuckooFilter = (*RedisCuckooFilter)(nil)
// )

// Type ''go generate'' to build mock using mockery
//go:generate go install -v github.com/vektra/mockery/v2/...@latest
//go:generate mockery --dir=.. --name=RedisCRUD --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisBloomFilter --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisCuckooFilter --case snake --outpkg redismock --output .
