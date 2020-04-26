package mapping

import (
	"errors"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/query"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// AssignmentContext contains references to all potential assignment
// destinations of a given mapping.
type AssignmentContext struct {
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
	Key string
}

func (v *metaAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	if _, deleted := value.(query.Delete); deleted {
		ctx.Meta.Delete(v.Key)
	} else {
		ctx.Meta.Set(v.Key, query.IToString(value))
	}
	return nil
}

//------------------------------------------------------------------------------

type jsonAssignment struct {
	Path []string
}

func (v *jsonAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	_, deleted := value.(query.Delete)
	if len(v.Path) == 0 {
		if deleted {
			return errors.New("cannot delete root of document")
		}
		ctx.Value = &value
	}
	gObj := gabs.Wrap(*ctx.Value)
	if deleted {
		gObj.Delete(v.Path...)
	} else {
		gObj.Set(value, v.Path...)
	}
	return nil
}

//------------------------------------------------------------------------------
