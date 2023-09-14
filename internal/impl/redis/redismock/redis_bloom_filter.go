// Code generated by mockery v2.33.2. DO NOT EDIT.

package redismock

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	redis "github.com/redis/go-redis/v9"
)

// RedisBloomFilter is an autogenerated mock type for the RedisBloomFilter type
type RedisBloomFilter struct {
	mock.Mock
}

// BFAdd provides a mock function with given fields: ctx, key, element
func (_m *RedisBloomFilter) BFAdd(ctx context.Context, key string, element interface{}) *redis.BoolCmd {
	ret := _m.Called(ctx, key, element)

	var r0 *redis.BoolCmd
	if rf, ok := ret.Get(0).(func(context.Context, string, interface{}) *redis.BoolCmd); ok {
		r0 = rf(ctx, key, element)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*redis.BoolCmd)
		}
	}

	return r0
}

// BFExists provides a mock function with given fields: ctx, key, element
func (_m *RedisBloomFilter) BFExists(ctx context.Context, key string, element interface{}) *redis.BoolCmd {
	ret := _m.Called(ctx, key, element)

	var r0 *redis.BoolCmd
	if rf, ok := ret.Get(0).(func(context.Context, string, interface{}) *redis.BoolCmd); ok {
		r0 = rf(ctx, key, element)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*redis.BoolCmd)
		}
	}

	return r0
}

// BFMAdd provides a mock function with given fields: ctx, key, elements
func (_m *RedisBloomFilter) BFMAdd(ctx context.Context, key string, elements ...interface{}) *redis.BoolSliceCmd {
	var _ca []interface{}
	_ca = append(_ca, ctx, key)
	_ca = append(_ca, elements...)
	ret := _m.Called(_ca...)

	var r0 *redis.BoolSliceCmd
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) *redis.BoolSliceCmd); ok {
		r0 = rf(ctx, key, elements...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*redis.BoolSliceCmd)
		}
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *RedisBloomFilter) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewRedisBloomFilter creates a new instance of RedisBloomFilter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRedisBloomFilter(t interface {
	mock.TestingT
	Cleanup(func())
}) *RedisBloomFilter {
	mock := &RedisBloomFilter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
