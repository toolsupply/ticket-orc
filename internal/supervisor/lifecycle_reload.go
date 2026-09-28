package supervisor

import (
	"context"
	"sort"
)

type ReloadHooks[W any] struct {
	ReplaceWatchers func(context.Context) error
	CompareWorker   func(effective, desired W) bool
	Commit          func(workers []W, running map[string]bool) string
}

type ReloadChange struct {
	Worker string
}

type ReloadTransition[W any] struct {
	Workers  []W
	Running  map[string]bool
	Changes  []ReloadChange
	Revision string
}

func (m *LifecycleManager[W, C]) SetWorker(name string, worker W) {
	m.gate.AddWorker(name)
	m.mu.Lock()
	m.workers[name] = worker
	m.mu.Unlock()
}

func (m *LifecycleManager[W, C]) ReplaceWorkers(workers map[string]W, removeMissing bool) {
	for name := range workers {
		m.gate.AddWorker(name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, worker := range workers {
		m.workers[name] = worker
	}
	if removeMissing {
		for name := range m.workers {
			if _, ok := workers[name]; !ok {
				delete(m.workers, name)
			}
		}
	}
}

// ApplyReload serializes candidate validation, watcher replacement, desired
// worker-state replacement, and the caller's runtime commit as one lifecycle
// transition. Candidate parsing and external preflight happen before this
// method; no active state changes unless every validation and watcher step
// succeeds.
func (m *LifecycleManager[W, C]) ApplyReload(ctx context.Context, candidate map[string]W, hooks ReloadHooks[W]) (ReloadTransition[W], error) {
	m.gate.Lock()
	defer m.gate.Unlock()
	if err := ctx.Err(); err != nil {
		return ReloadTransition[W]{}, err
	}
	m.mu.Lock()
	current := make(map[string]W, len(m.workers))
	children := make(map[string]C, len(m.children))
	paused := make(map[string]bool, len(m.paused))
	for name, worker := range m.workers {
		current[name] = worker
	}
	for name, child := range m.children {
		children[name] = child
	}
	for name, isPaused := range m.paused {
		if isPaused {
			paused[name] = true
		}
	}
	m.mu.Unlock()

	if len(candidate) == 0 && len(current) > 0 {
		return ReloadTransition[W]{}, &LifecycleError{Code: "invalid_config", Message: "candidate removes all configured workers"}
	}
	for name := range current {
		if _, remains := candidate[name]; remains {
			continue
		}
		if _, running := children[name]; running || paused[name] {
			return ReloadTransition[W]{}, &LifecycleError{Code: "invalid_config", Message: "candidate removes a running worker", Conflict: true}
		}
	}
	if hooks.ReplaceWatchers != nil {
		if err := hooks.ReplaceWatchers(ctx); err != nil {
			return ReloadTransition[W]{}, err
		}
	}

	workers := make([]W, 0, len(candidate))
	for _, worker := range candidate {
		workers = append(workers, worker)
	}
	sort.Slice(workers, func(i, j int) bool { return m.name(workers[i]) < m.name(workers[j]) })
	running := make(map[string]bool, len(children))
	changes := make([]ReloadChange, 0)
	for name, child := range children {
		desired, configured := candidate[name]
		if !configured {
			continue
		}
		running[name] = true
		if hooks.CompareWorker == nil || m.hooks.ChildWorker == nil {
			continue
		}
		if hooks.CompareWorker(m.hooks.ChildWorker(child), desired) {
			changes = append(changes, ReloadChange{Worker: name})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Worker < changes[j].Worker })
	workersByName := make(map[string]W, len(candidate))
	for name, worker := range candidate {
		workersByName[name] = worker
	}
	m.ReplaceWorkers(workersByName, true)
	revision := ""
	if hooks.Commit != nil {
		revision = hooks.Commit(workers, running)
	}
	return ReloadTransition[W]{Workers: workers, Running: running, Changes: changes, Revision: revision}, nil
}
