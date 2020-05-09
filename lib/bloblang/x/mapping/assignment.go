package mapping

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/query"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// AssignmentContext contains references to all potential assignment
// destinations of a given mapping.
type AssignmentContext struct {
	Maps  map[string]query.Function
	Vars  map[string]interface{}
	Meta  types.Metadata
	Value *interface{}
}

// Assignment represents a way of assigning a queried value to something within
// an assignment context. This could be a Benthos message, a variable, a
// metadata field, etc.
type Assignment interface {
	Apply(value interface{}, ctx AssignmentContext) error
}

//------------------------------------------------------------------------------

type varAssignment struct {
	Name string
}

func (v *varAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	if _, deleted := value.(query.Delete); deleted {
		delete(ctx.Vars, v.Name)
	} else {
		ctx.Vars[v.Name] = value
	}
	return nil
}

//------------------------------------------------------------------------------

type metaAssignment struct {
	Key *string
}

func (v *metaAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	_, deleted := value.(query.Delete)
	if v.Key == nil {
		if deleted {
			ctx.Meta.Iter(func(k, _ string) error {
				ctx.Meta.Delete(k)
				return nil
			})
		} else {
			if m, ok := value.(map[string]interface{}); ok {
				for k, v := range m {
					ctx.Meta.Set(k, query.IToString(v))
				}
			} else {
				return fmt.Errorf("setting root meta object requires object value, received: %T", value)
			}
		}
		return nil
	}
	if deleted {
		ctx.Meta.Delete(*v.Key)
	} else {
		ctx.Meta.Set(*v.Key, query.IToString(value))
	}
	return nil
}

//------------------------------------------------------------------------------

type jsonAssignment struct {
	Path []string
}

func (v *jsonAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	_, deleted := value.(query.Delete)
	if !deleted {
		value = query.IClone(value)
	}
	if len(v.Path) == 0 {
		if deleted {
			return errors.New("cannot delete root of document")
		}
		*ctx.Value = value
	}
	if _, isNothing := (*ctx.Value).(query.Nothing); isNothing || *ctx.Value == nil {
		*ctx.Value = map[string]interface{}{}
	}
	gObj := gabs.Wrap(*ctx.Value)
	if deleted {
		gObj.Delete(v.Path...)
	} else {
		gObj.Set(value, v.Path...)
	}
	*ctx.Value = gObj.Data()
	return nil
}

//------------------------------------------------------------------------------
