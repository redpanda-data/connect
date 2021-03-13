package query

// TargetType represents a query target type, which is a source of information
// that a query might require, such as metadata, structured message contents,
// the context, etc.
type TargetType int

// TargetTypes
const (
	TargetMetadata TargetType = iota
	TargetValue
	TargetVariable
)

// TargetPath represents a target type and segmented path that a query function
// references. An empty path indicates the root of the type is targetted.
type TargetPath struct {
	Type TargetType
	Path []string
}

// NewTargetPath constructs a new target path from a type and zero or more path
// segments.
func NewTargetPath(t TargetType, Path ...string) TargetPath {
	return TargetPath{
		Type: t,
		Path: Path,
	}
}

func aggregateTargetPaths(fns ...Function) func(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		var paths []TargetPath
		for _, fn := range fns {
			if fn != nil {
				_, tmpPaths := fn.QueryTargets(ctx)
				paths = append(paths, tmpPaths...)
			}
		}
		return ctx, paths
	}
}

func methodTargetPaths(target, method Function) func(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		methodCtx, targets := target.QueryTargets(ctx)
		returnCtx, methodTargets := method.QueryTargets(methodCtx)
		return returnCtx, append(targets, methodTargets...)
	}
}

// TargetsContext provides access to a range of query targets for functions to
// reference when determining their targets.
type TargetsContext struct {
	Maps          map[string]Function
	CurrentValues []TargetPath

	mainContext  []TargetPath
	namedContext *namedContextPath
}

type namedContextPath struct {
	name  string
	paths []TargetPath
	next  *namedContextPath
}

// NamedContext returns the path of a named context if it exists.
func (ctx TargetsContext) NamedContext(name string) []TargetPath {
	current := ctx.namedContext
	for current != nil {
		if current.name == name {
			return current.paths
		}
		current = current.next
	}
	return nil
}

// MainContext returns the path of the main context.
func (ctx TargetsContext) MainContext() []TargetPath {
	return ctx.mainContext
}

// WithMainContext returns a TargetsContext with a new main context path.
func (ctx TargetsContext) WithMainContext(paths []TargetPath) TargetsContext {
	ctx.mainContext = paths
	return ctx
}

// WithNamedContext returns a TargetsContext with a named value path.
func (ctx TargetsContext) WithNamedContext(name string, paths []TargetPath) TargetsContext {
	previous := ctx.namedContext
	ctx.namedContext = &namedContextPath{
		name:  name,
		paths: paths,
		next:  previous,
	}
	return ctx
}
