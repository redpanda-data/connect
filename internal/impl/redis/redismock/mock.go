package redismock

// Type ''go generate'' to build mock using mockery
//go:generate go install -v github.com/vektra/mockery/v2/...@latest
//go:generate mockery --dir=.. --name=RedisCRUD --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisBloomFilter --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisCuckooFilter --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisCacheAdaptor --case snake --outpkg redismock --output .
//go:generate mockery --dir=.. --name=RedisMultiCacheAdaptor --case snake --outpkg redismock --output .
