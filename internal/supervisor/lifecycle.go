package supervisor

import "sync"

// LifecycleCoordinator owns the ordering locks for supervisor transitions.
// Callers acquire the process-wide lock before a per-worker lock and only then
// inspect their smaller manager-state lock (see LOCKING.md).
type LifecycleCoordinator struct {
	global  sync.Mutex
	mu      sync.Mutex
	workers map[string]*sync.Mutex
}

func NewLifecycleCoordinator(workerNames []string) *LifecycleCoordinator {
	c := &LifecycleCoordinator{workers: make(map[string]*sync.Mutex, len(workerNames))}
	for _, name := range workerNames {
		c.AddWorker(name)
	}
	return c
}

func (c *LifecycleCoordinator) Lock() {
	if c != nil {
		c.global.Lock()
	}
}
func (c *LifecycleCoordinator) Unlock() {
	if c != nil {
		c.global.Unlock()
	}
}

func (c *LifecycleCoordinator) AddWorker(name string) {
	if c == nil || name == "" {
		return
	}
	c.mu.Lock()
	if c.workers[name] == nil {
		c.workers[name] = &sync.Mutex{}
	}
	c.mu.Unlock()
}

func (c *LifecycleCoordinator) WorkerLock(name string) *sync.Mutex {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workers[name]
}
