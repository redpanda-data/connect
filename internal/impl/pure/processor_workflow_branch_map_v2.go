package pure

import (
	"context"
	"fmt"
	"regexp"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/public/service"
)

type workflowBranchMapV2 struct {
	Branches     map[string]*Branch
	dependencies map[string][]string
}

// Locks all branches contained in the branch map and returns the latest DAG, a
// map of resources, and a func to unlock the resources that were locked. If
// any error occurs in locked each branch (the resource is missing, or the DAG
// is malformed) then an error is returned instead.
func (w *workflowBranchMapV2) LockV2() (branches map[string]*Branch, dependencies map[string][]string, unlockFn func(), err error) {
	return w.Branches, w.dependencies, func() {}, nil
}

func (w *workflowBranchMapV2) Close(ctx context.Context) error {
	for _, c := range w.Branches {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

var processDAGStageNameV2 = regexp.MustCompile("[a-zA-Z0-9-_]+")

func newWorkflowBranchMapV2(conf *service.ParsedConfig, mgr bundle.NewManagement) (*workflowBranchMapV2, error) {
	branchObjMap, err := conf.FieldObjectMap(wflowProcFieldBranchesV2)
	if err != nil {
		return nil, err
	}

	branches := map[string]*Branch{}
	for k, v := range branchObjMap {
		if len(processDAGStageNameV2.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow branch name '%v' contains invalid characters", k)
		}

		child, err := newBranchFromParsed(v, mgr.IntoPath("workflow", "branches", k))
		if err != nil {
			return nil, err
		}
		branches[k] = child
	}

	dependencies := make(map[string][]string)

	for k, v := range branchObjMap {
		depList, _ := v.FieldStringList("dependency_list")
		dependencies[k] = append(dependencies[k], depList...)
		if len(depList) == 0 {
			dependencies[k] = nil
		}
	}

	return &workflowBranchMapV2{
		Branches:     branches,
		dependencies: dependencies,
	}, nil
}
