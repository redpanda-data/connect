package pure

import (
	"context"
	"fmt"
	"regexp"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/public/service"
)

type workflowBranchV2 interface {
	lock() (*Branch, func())
}

//------------------------------------------------------------------------------

type workflowBranchMapV2 struct {
	dag            [][]string
	staticBranches map[string]*Branch

	dynamicBranches map[string]workflowBranchV2
}

func lockAllV2(dynBranches map[string]workflowBranchV2) (branches map[string]*Branch, unlockFn func(), err error) {
	unlocks := make([]func(), 0, len(dynBranches))
	unlockFn = func() {
		for _, u := range unlocks {
			if u != nil {
				u()
			}
		}
	}

	branches = make(map[string]*Branch, len(dynBranches))
	for k, v := range dynBranches {
		var branchUnlock func()
		branches[k], branchUnlock = v.lock()
		unlocks = append(unlocks, branchUnlock)
		if branches[k] == nil {
			err = fmt.Errorf("missing branch resource: %v", k)
			unlockFn()
			return
		}
	}
	return
}

// Locks all branches contained in the branch map and returns the latest DAG, a
// map of resources, and a func to unlock the resources that were locked. If
// any error occurs in locked each branch (the resource is missing, or the DAG
// is malformed) then an error is returned instead.
func (w *workflowBranchMapV2) LockV2() (dag [][]string, branches map[string]*Branch, unlockFn func(), err error) {
	return w.dag, w.staticBranches, func() {}, nil
}

func (w *workflowBranchMapV2) Close(ctx context.Context) error {
	for _, c := range w.staticBranches {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

var processDAGStageNameV2 = regexp.MustCompile("[a-zA-Z0-9-_]+")

func newWorkflowBranchMapV2(conf *service.ParsedConfig, mgr bundle.NewManagement) (*workflowBranchMapV2, error) {
	branchObjMap, err := conf.FieldObjectMap(wflowProcFieldBranchesV2)
	if err != nil {
		return nil, err
	}

	staticBranches := map[string]*Branch{}
	for k, v := range branchObjMap {
		if len(processDAGStageNameV2.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow branch name '%v' contains invalid characters", k)
		}

		child, err := newBranchFromParsed(v, mgr.IntoPath("workflow", "branches", k))
		if err != nil {
			return nil, err
		}
		staticBranches[k] = child
	}

	dag, err := conf.FieldStringListOfLists(wflowProcFieldAdjacencyMatrixV2)
	if err != nil {
		return nil, err
	}

	return &workflowBranchMapV2{
		dag:            dag,
		staticBranches: staticBranches,
	}, nil
}
