package processor

import (
	"errors"
	"fmt"
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
	targetsUsed() [][]string
	targetsProvided() [][]string
	createResult([]types.Part, types.Message) ([]types.Part, []branchMapError, error)
	overlayResult(types.Message, []types.Part) ([]branchMapError, error)
	CloseAsync()
	WaitForClose(time.Duration) error

	// Returns a closure that unlocks the branch resource, and a boolean that is
	// true if the underlying branch has actually changed since the last lock,
	// which means the dependency graph ought to be re-calculated.
	lock() (func(), bool)
}

//------------------------------------------------------------------------------

type workflowBranchMap struct {
	static   bool
	dag      [][]string
	branches map[string]workflowBranch
	m        sync.Mutex
}

// Locks all branches contained in the branch map and returns the latest DAG, a
// map of resources, and a func to unlock the resources that were locked. If
// any error occurs in locked each branch (the resource is missing, or the DAG
// is malformed) then an error is returned instead.
func (w *workflowBranchMap) Lock() ([][]string, map[string]workflowBranch, func(), error) {
	// Only allow one processing thread to mutate the cached DAG at a time, but
	// once they're resolved allow any number of threads to keep a reference to
	// the branch resources.
	w.m.Lock()
	defer w.m.Unlock()

	var unlocks []func()
	unlockFn := func() {
		for _, u := range unlocks {
			u()
		}
	}
	needsRefresh := false
	for _, b := range w.branches {
		fn, r := b.lock()
		if fn != nil {
			// Try not to allocate unlocks unless there are actual resources to
			// unlock.
			if unlocks == nil {
				unlocks = make([]func(), 0, len(w.branches))
			}
			unlocks = append(unlocks, fn)
		}
		needsRefresh = needsRefresh || r
	}
	if w.static {
		return w.dag, w.branches, unlockFn, nil
	}

	if len(w.dag) == 0 || needsRefresh {
		var err error
		if w.dag, err = resolveDynamicBranchDAG(w.branches); err != nil {
			unlockFn()
			return nil, nil, nil, fmt.Errorf("failed to resolve DAG: %w", err)
		}
	}
	return w.dag, w.branches, unlockFn, nil
}

func (w *workflowBranchMap) CloseAsync() {
	for _, b := range w.branches {
		b.CloseAsync()
	}
}

func (w *workflowBranchMap) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range w.branches {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

func newWorkflowBranchMap(conf WorkflowConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*workflowBranchMap, error) {
	children := map[string]workflowBranch{}
	for k, v := range conf.Branches {
		if len(processDAGStageName.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow branch name '%v' contains invalid characters", k)
		}

		bMgr, bLog, bStats := interop.LabelChild(k, mgr, log, stats)
		child, err := newBranch(v, bMgr, bLog, bStats)
		if err != nil {
			return nil, fmt.Errorf("failed to create branch '%v': %v", k, err)
		}

		children[k] = &normalBranch{child}
	}

	// TODO: V4 Remove this
	pProvider, _ := mgr.(procProvider)
	checkResource := func(key string) error {
		if pProvider == nil {
			return errors.New("manager does not support processor resources")
		}
		if p, err := pProvider.GetProcessor(key); err != nil {
			return fmt.Errorf("branch resource not found: %v", key)
		} else if _, ok := p.(*Branch); p != nil && !ok {
			return fmt.Errorf(
				"found resource named '%v' with wrong type, expected a branch processor, found: %T",
				key, p,
			)
		}
		return nil
	}

	for _, k := range conf.BranchResources {
		if _, exists := children[k]; exists {
			return nil, fmt.Errorf("branch resource name '%v' collides with an explicit branch", k)
		}
		if err := checkResource(k); err != nil {
			return nil, err
		}
		children[k] = &resourcedBranch{
			name: k,
			mgr:  pProvider,
		}
	}

	// When order is specified we infer that names missing from our explicit
	// branches are resources.
	for _, tier := range conf.Order {
		for _, k := range tier {
			if _, exists := children[k]; !exists {
				if err := checkResource(k); err != nil {
					return nil, err
				}
				children[k] = &resourcedBranch{
					name: k,
					mgr:  pProvider,
				}
			}
		}
	}

	var dag [][]string
	if len(conf.Order) > 0 {
		dag = conf.Order
		if err := verifyStaticBranchDAG(dag, children); err != nil {
			return nil, err
		}
	} else if len(conf.BranchResources) == 0 {
		var err error
		if dag, err = resolveDynamicBranchDAG(children); err != nil {
			return nil, err
		}
		log.Infof("Automatically resolved workflow DAG: %v\n", dag)
	}

	return &workflowBranchMap{
		static:   len(dag) > 0,
		dag:      dag,
		branches: children,
	}, nil
}

//------------------------------------------------------------------------------

type resourcedBranch struct {
	name string
	mgr  procProvider

	p types.Processor
}

func (r *resourcedBranch) targetsUsed() [][]string {
	if r.p == nil {
		return nil
	}
	b, ok := r.p.(*Branch)
	if !ok {
		return nil
	}
	return b.targetsUsed()
}

func (r *resourcedBranch) targetsProvided() [][]string {
	if r.p == nil {
		return nil
	}
	b, ok := r.p.(*Branch)
	if !ok {
		return nil
	}
	return b.targetsProvided()
}

func (r *resourcedBranch) createResult(parts []types.Part, referenceMsg types.Message) ([]types.Part, []branchMapError, error) {
	if r.p == nil {
		return nil, nil, fmt.Errorf("failed to obtain branch resource '%v'", r.name)
	}
	b, ok := r.p.(*Branch)
	if !ok {
		return nil, nil, fmt.Errorf("branch resource '%v' found incorrect processor type %T", r.name, r.p)
	}
	return b.createResult(parts, referenceMsg)
}

func (r *resourcedBranch) overlayResult(msg types.Message, parts []types.Part) ([]branchMapError, error) {
	if r.p == nil {
		return nil, fmt.Errorf("failed to obtain branch resource '%v'", r.name)
	}
	b, ok := r.p.(*Branch)
	if !ok {
		return nil, fmt.Errorf("branch resource '%v' found incorrect processor type %T", r.name, r.p)
	}
	return b.overlayResult(msg, parts)
}

// TODO: Expand this once manager supports locking and updates.
func (r *resourcedBranch) lock() (func(), bool) {
	prevP := r.p
	r.p, _ = r.mgr.GetProcessor(r.name)
	return func() {}, r.p != prevP
}

// Not needed as the manager handles shut down.
func (r *resourcedBranch) CloseAsync() {}
func (r *resourcedBranch) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

type normalBranch struct {
	*Branch
}

func (r *normalBranch) lock() (func(), bool) {
	return nil, false
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

func getBranchDeps(id string, wanted [][]string, branches map[string]workflowBranch) []string {
	dependencies := []string{}

eLoop:
	for k, b := range branches {
		if k == id {
			continue
		}
		for _, tp := range b.targetsProvided() {
			for _, tn := range wanted {
				if depHasPrefix(tn, tp) {
					dependencies = append(dependencies, k)
					continue eLoop
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

func resolveDynamicBranchDAG(branches map[string]workflowBranch) ([][]string, error) {
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
