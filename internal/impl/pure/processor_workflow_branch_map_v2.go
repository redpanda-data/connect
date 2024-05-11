package pure

import (
	"context"
	"fmt"
	"regexp"
	"sort"

	"github.com/quipo/dependencysolver"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/public/service"
)

type workflowBranchV2 interface {
	lock() (*Branch, func())
}

//------------------------------------------------------------------------------

type workflowBranchMapV2 struct {
	static         bool
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
	if w.static {
		return w.dag, w.staticBranches, func() {}, nil
	}

	if branches, unlockFn, err = lockAllV2(w.dynamicBranches); err != nil {
		return
	}
	if len(w.dag) > 0 {
		dag = w.dag
		return
	}

	if dag, err = resolveDynamicBranchDAGV2(branches); err != nil {
		unlockFn()
		err = fmt.Errorf("failed to resolve DAG: %w", err)
	}
	return
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

	dynamicBranches, staticBranches := map[string]workflowBranchV2{}, map[string]*Branch{}
	for k, v := range branchObjMap {
		if len(processDAGStageNameV2.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow branch name '%v' contains invalid characters", k)
		}

		child, err := newBranchFromParsed(v, mgr.IntoPath("workflow", "branches", k))
		if err != nil {
			return nil, err
		}

		dynamicBranches[k] = &normalBranchV2{child}
		staticBranches[k] = child
	}

	dag, err := conf.FieldStringListOfLists(wflowProcFieldAdjacencyMatrixV2)
	if err != nil {
		return nil, err
	}

	static := len(dynamicBranches) == len(staticBranches)

	return &workflowBranchMapV2{
		static:          static,
		dag:             dag,
		staticBranches:  staticBranches,
		dynamicBranches: dynamicBranches,
	}, nil
}

//------------------------------------------------------------------------------

type normalBranchV2 struct {
	*Branch
}

func (r *normalBranchV2) lock() (branch *Branch, unlockFn func()) {
	return r.Branch, nil
}

//------------------------------------------------------------------------------

func depHasPrefixV2(wanted, provided []string) bool {
	if len(wanted) < len(provided) {
		return false
	}
	for i, s := range provided {
		if wanted[i] != s {
			return false
		}
	}
	return true
}

func getBranchDepsV2(id string, wanted [][]string, branches map[string]*Branch) []string {
	dependencies := []string{}

	for k, b := range branches {
		if k == id {
			continue
		}
		for _, tp := range b.targetsProvided() {
			for _, tn := range wanted {
				if depHasPrefixV2(tn, tp) {
					dependencies = append(dependencies, k)
					break
				}
			}
		}
	}

	return dependencies
}

func verifyStaticBranchDAGV2(order [][]string, branches map[string]workflowBranchV2) error {
	remaining := map[string]struct{}{}
	seen := map[string]struct{}{}
	for id := range branches {
		remaining[id] = struct{}{}
	}
	for i, tier := range order {
		if len(tier) == 0 {
			return fmt.Errorf("explicit order tier '%v' was empty", i)
		}
		for _, t := range tier {
			if _, exists := seen[t]; exists {
				return fmt.Errorf("branch specified in order listed multiple times: %v", t)
			}
			seen[t] = struct{}{}
			delete(remaining, t)
		}
	}
	if len(remaining) > 0 {
		names := make([]string, 0, len(remaining))
		for k := range remaining {
			names = append(names, k)
		}
		return fmt.Errorf("the following branches were missing from order: %v", names)
	}
	return nil
}

func resolveDynamicBranchDAGV2(branches map[string]*Branch) ([][]string, error) {
	if len(branches) == 0 {
		return [][]string{}, nil
	}
	remaining := map[string]struct{}{}

	var entries []dependencysolver.Entry
	for id, b := range branches {
		wanted := b.targetsUsed()

		remaining[id] = struct{}{}
		entries = append(entries, dependencysolver.Entry{
			ID: id, Deps: getBranchDepsV2(id, wanted, branches),
		})
	}

	layers := dependencysolver.LayeredTopologicalSort(entries)
	for _, l := range layers {
		for _, id := range l {
			delete(remaining, id)
		}
	}

	if len(remaining) > 0 {
		var tProcs []string
		for k := range remaining {
			tProcs = append(tProcs, k)
		}
		sort.Strings(tProcs)
		return nil, fmt.Errorf("failed to automatically resolve DAG, circular dependencies detected for branches: %v", tProcs)
	}

	return layers, nil
}
