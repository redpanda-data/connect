// Code generated by mockery v2.33.2. DO NOT EDIT.

package redismock

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	redis "github.com/redis/go-redis/v9"
)

// RedisCuckooFilter is an autogenerated mock type for the RedisCuckooFilter type
type RedisCuckooFilter struct {
	mock.Mock
}

// CFAdd provides a mock function with given fields: ctx, key, element
func (_m *RedisCuckooFilter) CFAdd(ctx context.Context, key string, element interface{}) *redis.BoolCmd {
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

// CFAddNX provides a mock function with given fields: ctx, key, element
func (_m *RedisCuckooFilter) CFAddNX(ctx context.Context, key string, element interface{}) *redis.BoolCmd {
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

// CFDel provides a mock function with given fields: ctx, key, element
func (_m *RedisCuckooFilter) CFDel(ctx context.Context, key string, element interface{}) *redis.BoolCmd {
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

// CFExists provides a mock function with given fields: ctx, key, element
func (_m *RedisCuckooFilter) CFExists(ctx context.Context, key string, element interface{}) *redis.BoolCmd {
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

// CFInsert provides a mock function with given fields: ctx, key, options, elements
func (_m *RedisCuckooFilter) CFInsert(ctx context.Context, key string, options *redis.CFInsertOptions, elements ...interface{}) *redis.BoolSliceCmd {
	var _ca []interface{}
	_ca = append(_ca, ctx, key, options)
	_ca = append(_ca, elements...)
	ret := _m.Called(_ca...)

	var r0 *redis.BoolSliceCmd
	if rf, ok := ret.Get(0).(func(context.Context, string, *redis.CFInsertOptions, ...interface{}) *redis.BoolSliceCmd); ok {
		r0 = rf(ctx, key, options, elements...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*redis.BoolSliceCmd)
		}
	}

	return r0
}

// Close provides a mock function with given fields:
func (_m *RedisCuckooFilter) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewRedisCuckooFilter creates a new instance of RedisCuckooFilter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRedisCuckooFilter(t interface {
	mock.TestingT
	Cleanup(func())
}) *RedisCuckooFilter {
	mock := &RedisCuckooFilter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
