package processor

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/quipo/dependencysolver"
)

type workflowBranch interface {
	lock() (*Branch, func())
}

//------------------------------------------------------------------------------

type workflowBranchMap struct {
	static         bool
	dag            [][]string
	staticBranches map[string]*Branch

	dynamicBranches map[string]workflowBranch
}

func lockAll(dynBranches map[string]workflowBranch) (branches map[string]*Branch, unlockFn func(), err error) {
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
func (w *workflowBranchMap) Lock() (dag [][]string, branches map[string]*Branch, unlockFn func(), err error) {
	if w.static {
		return w.dag, w.staticBranches, func() {}, nil
	}

	if branches, unlockFn, err = lockAll(w.dynamicBranches); err != nil {
		return
	}
	if len(w.dag) > 0 {
		dag = w.dag
		return
	}

	if dag, err = resolveDynamicBranchDAG(branches); err != nil {
		unlockFn()
		err = fmt.Errorf("failed to resolve DAG: %w", err)
	}
	return
}

func (w *workflowBranchMap) CloseAsync() {
	for _, b := range w.staticBranches {
		b.CloseAsync()
	}
}

func (w *workflowBranchMap) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range w.staticBranches {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

var processDAGStageName = regexp.MustCompile("[a-zA-Z0-9-_]+")

func newWorkflowBranchMap(conf WorkflowConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*workflowBranchMap, error) {
	dynamicBranches, staticBranches := map[string]workflowBranch{}, map[string]*Branch{}
	for k, v := range conf.Branches {
		if len(processDAGStageName.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow branch name '%v' contains invalid characters", k)
		}

		bMgr, bLog, bStats := interop.LabelChild(k, mgr, log, stats)
		child, err := newBranch(v, bMgr, bLog, bStats)
		if err != nil {
			return nil, fmt.Errorf("failed to create branch '%v': %v", k, err)
		}

		dynamicBranches[k] = &normalBranch{child}
		staticBranches[k] = child
	}

	for _, k := range conf.BranchResources {
		if _, exists := dynamicBranches[k]; exists {
			return nil, fmt.Errorf("branch resource name '%v' collides with an explicit branch", k)
		}
		if err := interop.ProbeProcessor(context.Background(), mgr, k); err != nil {
			return nil, err
		}
		dynamicBranches[k] = &resourcedBranch{
			name: k,
			mgr:  mgr,
		}
	}

	// When order is specified we infer that names missing from our explicit
	// branches are resources.
	for _, tier := range conf.Order {
		for _, k := range tier {
			if _, exists := dynamicBranches[k]; !exists {
				if err := interop.ProbeProcessor(context.Background(), mgr, k); err != nil {
					return nil, err
				}
				dynamicBranches[k] = &resourcedBranch{
					name: k,
					mgr:  mgr,
				}
			}
		}
	}

	static := len(dynamicBranches) == len(staticBranches)

	var dag [][]string
	if len(conf.Order) > 0 {
		dag = conf.Order
		if err := verifyStaticBranchDAG(dag, dynamicBranches); err != nil {
			return nil, err
		}
	} else if static {
		var err error
		if dag, err = resolveDynamicBranchDAG(staticBranches); err != nil {
			return nil, err
		}
		log.Infof("Automatically resolved workflow DAG: %v\n", dag)
	}

	return &workflowBranchMap{
		static:          static,
		dag:             dag,
		staticBranches:  staticBranches,
		dynamicBranches: dynamicBranches,
	}, nil
}

//------------------------------------------------------------------------------

type resourcedBranch struct {
	name string
	mgr  types.Manager
}

func (r *resourcedBranch) lock() (branch *Branch, unlockFn func()) {
	var openOnce, releaseOnce sync.Once
	open, release := make(chan struct{}), make(chan struct{})
	unlockFn = func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}

	go func() {
		_ = interop.AccessProcessor(context.Background(), r.mgr, r.name, func(p types.Processor) {
			branch, _ = p.(*Branch)
			openOnce.Do(func() {
				close(open)
			})
			<-release
		})
		openOnce.Do(func() {
			close(open)
		})
	}()

	<-open
	return
}

//------------------------------------------------------------------------------

type normalBranch struct {
	*Branch
}

func (r *normalBranch) lock() (branch *Branch, unlockFn func()) {
	return r.Branch, nil
}

//------------------------------------------------------------------------------

func depHasPrefix(wanted, provided []string) bool {
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

func getBranchDeps(id string, wanted [][]string, branches map[string]*Branch) []string {
	dependencies := []string{}

	for k, b := range branches {
		if k == id {
			continue
		}
		for _, tp := range b.targetsProvided() {
			for _, tn := range wanted {
				if depHasPrefix(tn, tp) {
					dependencies = append(dependencies, k)
					break
				}
			}
		}
	}

	return dependencies
}

func verifyStaticBranchDAG(order [][]string, branches map[string]workflowBranch) error {
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

func resolveDynamicBranchDAG(branches map[string]*Branch) ([][]string, error) {
	if len(branches) == 0 {
		return [][]string{}, nil
	}
	remaining := map[string]struct{}{}

	var entries []dependencysolver.Entry
	for id, b := range branches {
		wanted := b.targetsUsed()

		remaining[id] = struct{}{}
		entries = append(entries, dependencysolver.Entry{
			ID: id, Deps: getBranchDeps(id, wanted, branches),
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
