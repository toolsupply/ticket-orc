package supervisor

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	RepositoryWatchDebounce   = 100 * time.Millisecond
	repositoryWatchBackoff    = 100 * time.Millisecond
	repositoryWatchMaxBackoff = 5 * time.Second
)

type RepositoryWatchEvent struct {
	Time   time.Time `json:"time"`
	Actor  *string   `json:"actor"`
	Ticket string    `json:"ticket"`
	Event  string    `json:"event"`
	State  string    `json:"state"`
	From   string    `json:"from"`
	To     string    `json:"to"`
}

type RepositoryWatchProcess interface {
	Wait() error
	Stop() error
}

type WatchProtocolError struct{ Cause error }

func (e *WatchProtocolError) Error() string {
	if e == nil || e.Cause == nil {
		return "Ticket watch protocol failure"
	}
	return "Ticket watch protocol failure: " + e.Cause.Error()
}
func (e *WatchProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// RepositoryWatchStarter returns a process only after the Ticket watch stream
// has crossed its validated READY boundary. Events may be delivered only
// after that boundary.
type RepositoryWatchStarter func(context.Context, ConfiguredRepository, func(RepositoryWatchEvent)) (RepositoryWatchProcess, error)
type RepositoryWatchProbe func(context.Context, ConfiguredRepository) (ticketclient.RepositoryInfo, error)

type repositoryStatusRuntime interface {
	SetRepositoryStatus(RepositoryStatus)
	RemoveRepositoryStatus(string)
	RepositoryStatuses() []RepositoryStatus
}

type repositoryWatchObserver struct {
	key         string
	repository  ConfiguredRepository
	cancel      context.CancelFunc
	mu          sync.Mutex
	process     RepositoryWatchProcess
	ready       bool
	attempt     uint64
	restarts    int
	lastRestart time.Time
}

// RepositoryWatchManager owns repository observer lifecycle, debounce state,
// and health transitions. Process creation remains an injected boundary so
// the application package does not own CLI child-process rendering or argv.
type RepositoryWatchManager struct {
	ctx       context.Context
	runtime   repositoryStatusRuntime
	probe     RepositoryWatchProbe
	start     RepositoryWatchStarter
	eventSink func(RuntimeEvent)
	readySink func(string)

	mu              sync.Mutex
	observers       map[string]*repositoryWatchObserver
	repositories    RepositoryRegistry
	replaceRevision uint64
	pending         map[string]RepositoryWatchEvent
	timers          map[string]*time.Timer
	started         bool
	stopped         bool
}

func NewRepositoryWatchManager(ctx context.Context, runtime repositoryStatusRuntime, repositories RepositoryRegistry, probe RepositoryWatchProbe, start RepositoryWatchStarter) *RepositoryWatchManager {
	manager := &RepositoryWatchManager{
		ctx: ctx, runtime: runtime, probe: probe, start: start,
		observers: make(map[string]*repositoryWatchObserver), repositories: CloneRepositoryRegistry(repositories),
		pending: make(map[string]RepositoryWatchEvent), timers: make(map[string]*time.Timer),
	}
	for key, repository := range repositories {
		manager.setStatusLocked(key, repository, RepositoryStatus{Key: key, State: "starting"})
	}
	return manager
}

func (m *RepositoryWatchManager) SetEventSink(sink func(RuntimeEvent)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.eventSink = sink
	m.mu.Unlock()
}

func (m *RepositoryWatchManager) SetReadySink(sink func(string)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.readySink = sink
	m.mu.Unlock()
}

func (m *RepositoryWatchManager) StartObservers() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.started || m.stopped {
		m.mu.Unlock()
		return
	}
	m.started = true
	repositories := CloneRepositoryRegistry(m.repositories)
	m.mu.Unlock()
	for key, repository := range repositories {
		m.addObserver(key, repository)
	}
}

func (m *RepositoryWatchManager) Replace(ctx context.Context, repositories RepositoryRegistry) error {
	return m.replace(ctx, 0, repositories)
}

// ReplaceAtRevision ignores an inventory that was superseded by a newer
// publication before it could be applied or start observers.
func (m *RepositoryWatchManager) ReplaceAtRevision(ctx context.Context, revision uint64, repositories RepositoryRegistry) error {
	if revision == 0 {
		return errors.New("repository observer revision must be positive")
	}
	return m.replace(ctx, revision, repositories)
}

func (m *RepositoryWatchManager) replace(ctx context.Context, revision uint64, repositories RepositoryRegistry) error {
	if m == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return errors.New("repository observers are stopped")
	}
	if revision == 0 {
		m.replaceRevision++
		revision = m.replaceRevision
	} else {
		if revision <= m.replaceRevision {
			m.mu.Unlock()
			return nil
		}
		m.replaceRevision = revision
	}
	toStop := make([]*repositoryWatchObserver, 0)
	for key, observer := range m.observers {
		candidate, ok := repositories[key]
		if !ok || !sameConfiguredRepository(observer.repository, candidate) {
			toStop = append(toStop, observer)
			delete(m.observers, key)
		}
	}
	if m.runtime != nil {
		for key := range m.repositories {
			if _, ok := repositories[key]; !ok {
				m.runtime.RemoveRepositoryStatus(key)
			}
		}
	}
	for key := range m.pending {
		previous, existed := m.repositories[key]
		candidate, remains := repositories[key]
		if existed && (!remains || !sameConfiguredRepository(previous, candidate)) {
			if timer := m.timers[key]; timer != nil {
				timer.Stop()
				delete(m.timers, key)
			}
			delete(m.pending, key)
		}
	}
	m.repositories = CloneRepositoryRegistry(repositories)
	started := m.started
	for key, repository := range repositories {
		if _, ok := m.observers[key]; !ok {
			m.setStatusLocked(key, repository, RepositoryStatus{Key: key, State: "starting"})
		}
	}
	m.mu.Unlock()
	for _, observer := range toStop {
		observer.cancel()
		observer.stopProcess()
	}
	if started {
		for key, repository := range repositories {
			m.addObserverAtRevision(key, repository, revision)
		}
	}
	return nil
}

func sameConfiguredRepository(left, right ConfiguredRepository) bool {
	return left.Target == right.Target && left.ID == right.ID
}

func (m *RepositoryWatchManager) addObserver(key string, repository ConfiguredRepository) {
	m.addObserverAtRevision(key, repository, 0)
}

func (m *RepositoryWatchManager) addObserverAtRevision(key string, repository ConfiguredRepository, revision uint64) {
	m.mu.Lock()
	if m.stopped || (revision != 0 && revision != m.replaceRevision) {
		m.mu.Unlock()
		return
	}
	if _, exists := m.observers[key]; exists {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(m.ctx)
	observer := &repositoryWatchObserver{key: key, repository: repository, cancel: cancel}
	m.observers[key] = observer
	m.mu.Unlock()
	go m.runObserver(ctx, observer)
}

func (m *RepositoryWatchManager) runObserver(ctx context.Context, observer *repositoryWatchObserver) {
	backoff := repositoryWatchBackoff
	for {
		attempt, active := m.beginObserverAttempt(ctx, observer)
		if !active {
			return
		}
		process, err := m.start(ctx, observer.repository, func(event RepositoryWatchEvent) { m.ticketChangedFrom(observer, attempt, event) })
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failure := "watch_start_failed"
			var protocolErr *WatchProtocolError
			if errors.As(err, &protocolErr) {
				failure = RepositoryWatchFailureCode(err)
			}
			m.markDegraded(observer, failure)
			if !waitRepositoryBackoff(ctx, backoff) {
				return
			}
			backoff = nextRepositoryBackoff(backoff)
			continue
		}
		recoveryCode, active := m.activateObserver(ctx, observer, attempt, process)
		if !active {
			return
		}
		m.publishReady(observer)
		if err := m.refresh(ctx, observer); err != nil {
			m.markDegraded(observer, "refresh_failed")
		} else if recoveryCode != "" {
			m.publishRecovery(observer, recoveryCode)
		}
		err = waitRepositoryProcess(ctx, process)
		observer.clearProcess(process)
		m.retireObserverAttempt(observer, attempt)
		if ctx.Err() != nil {
			return
		}
		m.markDegraded(observer, RepositoryWatchFailureCode(err))
		if !waitRepositoryBackoff(ctx, backoff) {
			return
		}
		backoff = nextRepositoryBackoff(backoff)
	}
}

func (m *RepositoryWatchManager) beginObserverAttempt(ctx context.Context, observer *repositoryWatchObserver) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil || m.observers[observer.key] != observer {
		return 0, false
	}
	observer.attempt++
	observer.ready = false
	return observer.attempt, true
}

func (m *RepositoryWatchManager) retireObserverAttempt(observer *repositoryWatchObserver, attempt uint64) {
	m.mu.Lock()
	if m.observers[observer.key] == observer && observer.attempt == attempt {
		observer.ready = false
	}
	m.mu.Unlock()
}

func waitRepositoryProcess(ctx context.Context, process RepositoryWatchProcess) error {
	result := make(chan error, 1)
	go func() { result <- process.Wait() }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		_ = process.Stop()
		return <-result
	}
}

func waitRepositoryBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextRepositoryBackoff(current time.Duration) time.Duration {
	if current >= repositoryWatchMaxBackoff/2 {
		return repositoryWatchMaxBackoff
	}
	return current * 2
}

func RepositoryWatchFailureCode(err error) string {
	if err == nil {
		return "watch_exit"
	}
	var protocolErr *WatchProtocolError
	if errors.As(err, &protocolErr) {
		return "watch_protocol_failed"
	}
	return "watch_exit"
}

func (o *repositoryWatchObserver) setProcess(process RepositoryWatchProcess) {
	o.mu.Lock()
	o.process = process
	o.mu.Unlock()
}
func (o *repositoryWatchObserver) clearProcess(process RepositoryWatchProcess) {
	o.mu.Lock()
	if o.process == process {
		o.process = nil
	}
	o.mu.Unlock()
}
func (o *repositoryWatchObserver) stopProcess() {
	o.mu.Lock()
	process := o.process
	o.mu.Unlock()
	if process != nil {
		_ = process.Stop()
	}
}

func (m *RepositoryWatchManager) refresh(ctx context.Context, observer *repositoryWatchObserver) error {
	if m.probe == nil {
		return nil
	}
	info, err := m.probe(ctx, observer.repository)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if ctx.Err() != nil || m.observers[observer.key] != observer {
		m.mu.Unlock()
		return context.Canceled
	}
	status := m.statusLocked(observer.key)
	status.Name = repositoryInfoName(info)
	status.Path = info.Path
	if status.State == "degraded" {
		status.State = "healthy"
	}
	m.setStatusLocked(observer.key, observer.repository, status)
	m.mu.Unlock()
	return nil
}

func repositoryInfoName(info ticketclient.RepositoryInfo) string {
	if info.Name == nil {
		return ""
	}
	return *info.Name
}

func (m *RepositoryWatchManager) TicketChanged(key string, event RepositoryWatchEvent) {
	m.mu.Lock()
	m.ticketChangedLocked(key, event)
	m.mu.Unlock()
}

func (m *RepositoryWatchManager) ticketChangedFrom(observer *repositoryWatchObserver, attempt uint64, event RepositoryWatchEvent) {
	m.mu.Lock()
	if observer == nil || m.observers[observer.key] != observer || observer.attempt != attempt || !observer.ready {
		m.mu.Unlock()
		return
	}
	m.ticketChangedLocked(observer.key, event)
	m.mu.Unlock()
}

func (m *RepositoryWatchManager) ticketChangedLocked(key string, event RepositoryWatchEvent) {
	status := m.statusLocked(key)
	status.LastEventAt = time.Now().UTC()
	if status.State != "degraded" {
		status.State = "healthy"
	}
	if repository, ok := m.repositories[key]; ok {
		m.setStatusLocked(key, repository, status)
	}
	m.pending[key] = event
	if timer := m.timers[key]; timer != nil {
		timer.Stop()
	}
	m.timers[key] = time.AfterFunc(RepositoryWatchDebounce, func() { m.flushChanged(key) })
}

func (m *RepositoryWatchManager) flushChanged(key string) {
	m.mu.Lock()
	event, ok := m.pending[key]
	repository, repositoryFound := m.repositories[key]
	delete(m.pending, key)
	delete(m.timers, key)
	sink := m.eventSink
	m.mu.Unlock()
	if !ok || !repositoryFound || repository.ID == "" || sink == nil {
		return
	}
	public := RuntimeEvent{Type: "ticket.repository_changed", RepositoryID: repository.ID, RepositoryKey: key, Code: event.Event, State: event.State}
	if ticketclient.ValidateFullID(event.Ticket) == nil {
		public.Ticket = event.Ticket
	}
	if event.Actor != nil {
		public.Actor = *event.Actor
	}
	sink(public)
}

func (m *RepositoryWatchManager) activateObserver(ctx context.Context, observer *repositoryWatchObserver, attempt uint64, process RepositoryWatchProcess) (string, bool) {
	m.mu.Lock()
	if ctx.Err() != nil || m.observers[observer.key] != observer || observer.attempt != attempt {
		m.mu.Unlock()
		_ = process.Stop()
		return "", false
	}
	observer.setProcess(process)
	observer.ready = true
	observer.mu.Lock()
	observer.restarts++
	observer.lastRestart = time.Now().UTC()
	restarted := observer.restarts > 1
	observer.mu.Unlock()
	status := m.statusLocked(observer.key)
	wasDegraded := status.State == "degraded"
	status.State = "healthy"
	status.Failure = ""
	if restarted {
		status.LastRestartAt = time.Now().UTC()
		status.RestartCount++
	}
	m.setStatusLocked(observer.key, observer.repository, status)
	m.mu.Unlock()
	if wasDegraded {
		return "observer_recovered", true
	}
	if restarted {
		return "observer_restarted", true
	}
	return "", true
}

func (m *RepositoryWatchManager) publishReady(observer *repositoryWatchObserver) {
	m.mu.Lock()
	if current, ok := m.observers[observer.key]; !ok || current != observer {
		m.mu.Unlock()
		return
	}
	sink := m.readySink
	repositoryID := observer.repository.ID
	m.mu.Unlock()
	if sink != nil {
		sink(repositoryID)
	}
}

func (m *RepositoryWatchManager) publishRecovery(observer *repositoryWatchObserver, code string) {
	m.mu.Lock()
	if current, ok := m.observers[observer.key]; !ok || current != observer {
		m.mu.Unlock()
		return
	}
	status := m.statusLocked(observer.key)
	sink := m.eventSink
	m.mu.Unlock()
	if sink != nil && status.State == "healthy" {
		sink(RuntimeEvent{Type: "ticket.repository_observer", RepositoryID: observer.repository.ID, RepositoryKey: observer.key, State: "healthy", Code: code})
	}
}

func (m *RepositoryWatchManager) markDegraded(observer *repositoryWatchObserver, failure string) {
	m.mu.Lock()
	if current, ok := m.observers[observer.key]; !ok || current != observer {
		m.mu.Unlock()
		return
	}
	status := m.statusLocked(observer.key)
	status.State = "degraded"
	status.Failure = failure
	m.setStatusLocked(observer.key, observer.repository, status)
	sink := m.eventSink
	m.mu.Unlock()
	if sink != nil {
		sink(RuntimeEvent{Type: "ticket.repository_observer", RepositoryID: observer.repository.ID, RepositoryKey: observer.key, State: "degraded", Code: failure})
	}
}

func (m *RepositoryWatchManager) statusLocked(key string) RepositoryStatus {
	if m.runtime == nil {
		return RepositoryStatus{Key: key}
	}
	for _, status := range m.runtime.RepositoryStatuses() {
		if status.Key == key {
			return status
		}
	}
	return RepositoryStatus{Key: key}
}

func (m *RepositoryWatchManager) setStatusLocked(key string, repository ConfiguredRepository, status RepositoryStatus) {
	status.ID = repository.ID
	status.Key = key
	if status.Path == "" {
		if repository.Info != nil {
			status.Path = repository.Info.Path
		}
	}
	if status.Name == "" && repository.Info != nil {
		status.Name = repositoryInfoName(*repository.Info)
	}
	if m.runtime != nil {
		m.runtime.SetRepositoryStatus(status)
	}
}

func (m *RepositoryWatchManager) StopObservers() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	for key, timer := range m.timers {
		timer.Stop()
		delete(m.timers, key)
		delete(m.pending, key)
	}
	observers := make([]*repositoryWatchObserver, 0, len(m.observers))
	for _, observer := range m.observers {
		observers = append(observers, observer)
	}
	m.observers = make(map[string]*repositoryWatchObserver)
	m.mu.Unlock()
	for _, observer := range observers {
		observer.cancel()
		observer.stopProcess()
	}
}

func (m *RepositoryWatchManager) Statuses() []RepositoryStatus {
	if m == nil || m.runtime == nil {
		return nil
	}
	result := m.runtime.RepositoryStatuses()
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}
