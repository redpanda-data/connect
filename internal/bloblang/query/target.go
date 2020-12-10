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

func aggregateTargetPaths(fns ...Function) func(ctx TargetsContext) []TargetPath {
	return func(ctx TargetsContext) []TargetPath {
		var paths []TargetPath
		for _, fn := range fns {
			if fn != nil {
				paths = append(paths, fn.QueryTargets(ctx)...)
			}
		}
		return paths
	}
}

// Rebase a collection of targets as though they are executing on the context
// of the base targets, but do not include the base targets themselves in the
// results.
func rebaseTargetPaths(paths, base []TargetPath) []TargetPath {
	var refValuePaths [][]string
	for _, refTarget := range base {
		if refTarget.Type == TargetValue {
			refValuePaths = append(refValuePaths, refTarget.Path)
		}
	}
	if len(refValuePaths) == 0 {
		return paths
	}

	var expandedPaths []TargetPath
	for _, target := range paths {
		if target.Type == TargetValue {
			for _, refValuePath := range refValuePaths {
				var newPath []string
				newPath = append(newPath, refValuePath...)
				newPath = append(newPath, target.Path...)
				expandedPaths = append(expandedPaths, NewTargetPath(TargetValue, newPath...))
			}
		} else {
			expandedPaths = append(expandedPaths, target)
		}
	}
	return expandedPaths
}

// Expand a slice of target paths with another, as if the base targets make up
// the context of those paths.
func expandTargetPaths(base, expand []TargetPath) []TargetPath {
	var baseValuePaths [][]string
	var expandedPaths []TargetPath
	for _, baseTarget := range base {
		if baseTarget.Type == TargetValue {
			baseValuePaths = append(baseValuePaths, baseTarget.Path)
		} else {
			expandedPaths = append(expandedPaths, baseTarget)
		}
	}

	for _, target := range expand {
		if target.Type == TargetValue && len(baseValuePaths) > 0 {
			for _, baseValuePath := range baseValuePaths {
				var newPath []string
				newPath = append(newPath, baseValuePath...)
				newPath = append(newPath, target.Path...)
				expandedPaths = append(expandedPaths, NewTargetPath(TargetValue, newPath...))
			}
		} else {
			expandedPaths = append(expandedPaths, target)
		}
	}
	return expandedPaths
}
