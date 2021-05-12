package query

import (
	"errors"
	"fmt"
)

var errEndOfIter = errors.New("iterator reached the end")

// Iterator allows traversal of a Bloblang function result in iterations.
type Iterator interface {
	// Next provides the next element of the iterator, or an error. When the
	// iterator has reached the end ErrEndOfIter is returned.
	Next() (interface{}, error)

	// Len provides a static length of the iterator when possible.
	Len() (int, bool)
}

// Iterable is an interface implemented by Bloblang functions that are able to
// expose their results as an interator, allowing for more efficient chaining of
// array based methods.
type Iterable interface {
	// TryIterate attempts to create an iterator that walks the function result.
	// Some functions will be unable to provide an iterator due to either the
	// context or function arguments provided, therefore it's possible that a
	// static value will be returned instead.
	TryIterate(ctx FunctionContext) (Iterator, interface{}, error)
}

//------------------------------------------------------------------------------

func execTryIter(iFn Iterable, fn Function, ctx FunctionContext) (iter Iterator, v interface{}, err error) {
	if iFn != nil {
		if iter, v, err = iFn.TryIterate(ctx); err != nil || iter != nil {
			return
		}
	} else if v, err = fn.Exec(ctx); err != nil {
		return
	}
	if arr, ok := v.([]interface{}); ok {
		return arrayIterator(arr), nil, nil
	}
	return
}

func arrayIterator(arr []interface{}) Iterator {
	return closureIterator{
		next: func() (interface{}, error) {
			if len(arr) == 0 {
				return nil, errEndOfIter
			}
			v := arr[0]
			arr = arr[1:]
			return v, nil
		},
		len: func() (int, bool) {
			return len(arr), true
		},
	}
}

func drainIter(iter Iterator) ([]interface{}, error) {
	var arr []interface{}
	if l, ok := iter.Len(); ok {
		arr = make([]interface{}, 0, l)
	}
	for {
		v, err := iter.Next()
		if err != nil {
			if err == errEndOfIter {
				return arr, nil
			}
			return nil, err
		}
		arr = append(arr, v)
	}
}

type closureIterator struct {
	next func() (interface{}, error)
	len  func() (int, bool)
}

func (c closureIterator) Next() (interface{}, error) {
	return c.next()
}

func (c closureIterator) Len() (int, bool) {
	if c.len == nil {
		return 0, false
	}
	return c.len()
}

//------------------------------------------------------------------------------

type filterMethod struct {
	target     Function
	iterTarget Iterable
	mapFn      Function
}

func newFilterMethod(target Function, args ...interface{}) (Function, error) {
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}
	iterTarget, _ := target.(Iterable)
	return &filterMethod{
		target:     target,
		iterTarget: iterTarget,
		mapFn:      mapFn,
	}, nil
}

func (f *filterMethod) Annotation() string {
	return "method filter"
}

func (f *filterMethod) TryIterate(ctx FunctionContext) (Iterator, interface{}, error) {
	iter, res, err := execTryIter(f.iterTarget, f.target, ctx)
	if err != nil {
		return nil, nil, err
	}
	if iter == nil {
		res, err = f.execFallback(ctx, res)
		return nil, res, err
	}
	return closureIterator{
		next: func() (interface{}, error) {
			for {
				v, err := iter.Next()
				if err != nil {
					if err != errEndOfIter {
						err = ErrFrom(err, f.target)
					}
					return nil, err
				}
				f, err := f.mapFn.Exec(ctx.WithValue(v))
				if err != nil {
					return nil, err
				}
				if b, _ := f.(bool); b {
					return v, nil
				}
			}
		},
	}, nil, nil
}

// We also support filtering objects, so when we're unable to spawn an iterator
// we attempt to process a map.
func (f *filterMethod) execFallback(ctx FunctionContext, res interface{}) (interface{}, error) {
	m, ok := res.(map[string]interface{})
	if !ok {
		return nil, ErrFrom(NewTypeError(res, ValueArray, ValueObject), f.target)
	}
	newMap := make(map[string]interface{}, len(m))
	for k, v := range m {
		var ctxMap interface{} = map[string]interface{}{
			"key":   k,
			"value": v,
		}
		f, err := f.mapFn.Exec(ctx.WithValue(ctxMap))
		if err != nil {
			return nil, err
		}
		if b, _ := f.(bool); b {
			newMap[k] = v
		}
	}
	return newMap, nil
}

func (f *filterMethod) Exec(ctx FunctionContext) (interface{}, error) {
	iter, res, err := f.TryIterate(ctx)
	if err != nil || res != nil {
		return res, err
	}
	return drainIter(iter)
}

func (f *filterMethod) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return f.target.QueryTargets(ctx)
}

//------------------------------------------------------------------------------

type mapEachMethod struct {
	target     Function
	iterTarget Iterable
	mapFn      Function
}

func newMapEachMethod(target Function, args ...interface{}) (Function, error) {
	mapFn, ok := args[0].(Function)
	if !ok {
		return nil, fmt.Errorf("expected query argument, received %T", args[0])
	}
	iterTarget, _ := target.(Iterable)
	return &mapEachMethod{
		target:     target,
		iterTarget: iterTarget,
		mapFn:      mapFn,
	}, nil
}

func (m *mapEachMethod) Annotation() string {
	return "method map_each"
}

func (m *mapEachMethod) TryIterate(ctx FunctionContext) (Iterator, interface{}, error) {
	iter, res, err := execTryIter(m.iterTarget, m.target, ctx)
	if err != nil {
		return nil, nil, err
	}
	if iter == nil {
		res, err = m.execFallback(ctx, res)
		return nil, res, err
	}
	return closureIterator{
		next: func() (interface{}, error) {
			for {
				v, err := iter.Next()
				if err != nil {
					if err != errEndOfIter {
						err = ErrFrom(err, m.target)
					}
					return nil, err
				}

				newV, err := m.mapFn.Exec(ctx.WithValue(v))
				if err != nil {
					return nil, ErrFrom(err, m.mapFn)
				}
				switch newV.(type) {
				case Delete:
				case Nothing:
					return v, nil
				default:
					return newV, nil
				}
			}
		},
	}, nil, nil
}

func (m *mapEachMethod) execFallback(ctx FunctionContext, res interface{}) (interface{}, error) {
	resMap, ok := res.(map[string]interface{})
	if !ok {
		return nil, ErrFrom(NewTypeError(res, ValueArray, ValueObject), m.target)
	}
	newMap := make(map[string]interface{}, len(resMap))
	for k, v := range resMap {
		var ctxMap interface{} = map[string]interface{}{
			"key":   k,
			"value": v,
		}
		newV, mapErr := m.mapFn.Exec(ctx.WithValue(ctxMap))
		if mapErr != nil {
			return nil, fmt.Errorf("failed to process element %v: %w", k, ErrFrom(mapErr, m.mapFn))
		}
		switch newV.(type) {
		case Delete:
		case Nothing:
			newMap[k] = v
		default:
			newMap[k] = newV
		}
	}
	return newMap, nil
}

func (m *mapEachMethod) Exec(ctx FunctionContext) (interface{}, error) {
	iter, res, err := m.TryIterate(ctx)
	if err != nil || res != nil {
		return res, err
	}
	return drainIter(iter)
}

func (m *mapEachMethod) QueryTargets(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return m.target.QueryTargets(ctx)
}
