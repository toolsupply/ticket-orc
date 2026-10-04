package cli

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/steertransport"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const (
	steerPollInterval     = time.Second
	steerOperationTimeout = 5 * time.Second
)

const steerClaimRecoveryPrompt = "[ticket-orc] This session replaces the previous session for this Ticket actor. There is already an active Ticket claim. Inspect the actor's current claim and continue it before taking new work. After resolving it, continue processing actionable work for the current actor until none remains."
const steerSessionBootstrapPrompt = "[ticket-orc] This session is registered for Orc-steered Ticket work.\n\nFor Orc-steered work, follow the installed ticket-orc-worker Skill and the Ticket Tasks Skill it references. If those instructions are not currently available or clear in this session, consult them before acting on Orc work. Ticket remains authoritative for work selection, ownership, and lifecycle."

type steerRolePolicy struct {
	TicketQueue      string
	QueueFilters     ticketclient.QueueFilters
	NudgePrompt      string
	ReviewCompletion string
	MaxBounces       int
	ContainmentActor string
}

type steerPolicySnapshot struct {
	generation uint64
	roles      map[string]steerRolePolicy
}

// steerPolicyStore publishes immutable steering policy generations. A
// dispatch reservation spans only the state transition and queue operation;
// Replace waits for existing reservations before making a new generation
// visible, without holding the mutex while external work runs.
type steerPolicyStore struct {
	mu         sync.Mutex
	changed    *sync.Cond
	generation uint64
	active     int
	roles      map[string]steerRolePolicy
}

func newSteerPolicyStore(roles map[string]steerRolePolicy) *steerPolicyStore {
	store := &steerPolicyStore{generation: 1, roles: cloneSteerRolePolicies(roles)}
	store.changed = sync.NewCond(&store.mu)
	return store
}

func cloneSteerRolePolicies(roles map[string]steerRolePolicy) map[string]steerRolePolicy {
	cloned := make(map[string]steerRolePolicy, len(roles))
	for name, policy := range roles {
		policy.QueueFilters.Tags = append([]string(nil), policy.QueueFilters.Tags...)
		policy.QueueFilters.WithoutTags = append([]string(nil), policy.QueueFilters.WithoutTags...)
		sort.Strings(policy.QueueFilters.Tags)
		sort.Strings(policy.QueueFilters.WithoutTags)
		cloned[name] = policy
	}
	return cloned
}

func (store *steerPolicyStore) Snapshot() steerPolicySnapshot {
	if store == nil {
		return steerPolicySnapshot{}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return steerPolicySnapshot{generation: store.generation, roles: cloneSteerRolePolicies(store.roles)}
}

func (store *steerPolicyStore) Replace(roles map[string]steerRolePolicy) (uint64, bool) {
	if store == nil {
		return 0, false
	}
	cloned := cloneSteerRolePolicies(roles)
	store.mu.Lock()
	if reflect.DeepEqual(store.roles, cloned) {
		generation := store.generation
		store.mu.Unlock()
		return generation, false
	}
	for store.active > 0 {
		store.changed.Wait()
	}
	// Waiting releases the mutex, so another replacement may have published
	// this same policy while active dispatch reservations drained.
	if reflect.DeepEqual(store.roles, cloned) {
		generation := store.generation
		store.mu.Unlock()
		return generation, false
	}
	store.generation++
	store.roles = cloned
	generation := store.generation
	store.mu.Unlock()
	return generation, true
}

func (store *steerPolicyStore) WithGeneration(generation uint64, operation func() error) (bool, error) {
	if store == nil {
		return false, nil
	}
	store.mu.Lock()
	if generation != store.generation {
		store.mu.Unlock()
		return false, nil
	}
	store.active++
	store.mu.Unlock()
	defer func() {
		store.mu.Lock()
		store.active--
		if store.active == 0 {
			store.changed.Broadcast()
		}
		store.mu.Unlock()
	}()
	return true, operation()
}

func steerReviewCompletionPrompt(policy steerRolePolicy) string {
	if policy.TicketQueue != "review" {
		return ""
	}
	if policy.ReviewCompletion == ReviewCompletionClose {
		return "[ticket-orc] For accepted work, approve and close the ticket."
	}
	return "[ticket-orc] For accepted work, approve the ticket to signoff."
}

type dynamicSteerStatus struct {
	mu      sync.RWMutex
	items   []daemon.SteerStatus
	publish func(daemon.Event)
	first   chan struct{}
	seen    bool
}

func (s *dynamicSteerStatus) waitFirst(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.seen {
		s.mu.Unlock()
		return nil
	}
	if s.first == nil {
		s.first = make(chan struct{})
	}
	first := s.first
	s.mu.Unlock()
	select {
	case <-first:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *dynamicSteerStatus) snapshot() []daemon.SteerStatus {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]daemon.SteerStatus(nil), s.items...)
}
func (s *dynamicSteerStatus) setPublisher(publish func(daemon.Event)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.publish = publish
	s.mu.Unlock()
}
func (s *dynamicSteerStatus) publishDelivery(reg state.SteerRegistration, stateName, ticket string) {
	if s == nil {
		return
	}
	s.mu.RLock()
	publish := s.publish
	s.mu.RUnlock()
	if publish == nil {
		return
	}
	publish(daemon.Event{
		Type: "steer.delivery", Role: reg.Role, Actor: reg.Actor, Harness: reg.Harness, Session: shortIdentity(reg.SessionID),
		State: stateName, Ticket: ticket, RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName,
	})
}
func (s *dynamicSteerStatus) replace(items []daemon.SteerStatus) {
	if s == nil {
		return
	}
	s.mu.Lock()
	previous := append([]daemon.SteerStatus(nil), s.items...)
	s.items = append([]daemon.SteerStatus(nil), items...)
	if !s.seen {
		s.seen = true
		if s.first != nil {
			close(s.first)
		}
	}
	publish := s.publish
	s.mu.Unlock()
	if publish == nil || sameSteerStatuses(previous, items) {
		return
	}
	previousByKey := make(map[string]daemon.SteerStatus, len(previous))
	for _, item := range previous {
		previousByKey[steerStatusKey(item)] = item
	}
	for _, item := range items {
		old, found := previousByKey[steerStatusKey(item)]
		if found && reflect.DeepEqual(old, item) {
			continue
		}
		publish(daemon.Event{Type: "steer.status", Role: item.Role, Actor: item.Actor, Harness: item.Harness, Session: shortIdentity(item.Session), State: item.State, Code: item.Code, Ticket: item.Ticket, RepositoryID: item.RepositoryID, RepositoryName: item.RepositoryName})
	}
	for _, old := range previous {
		found := false
		for _, item := range items {
			if item.RepositoryID == old.RepositoryID && item.Actor == old.Actor && item.Harness == old.Harness && item.Session == old.Session {
				found = true
				break
			}
		}
		if !found {
			publish(daemon.Event{Type: "steer.status", Role: old.Role, Actor: old.Actor, Harness: old.Harness, Session: shortIdentity(old.Session), State: "left", Code: "registration_removed", RepositoryID: old.RepositoryID, RepositoryName: old.RepositoryName})
		}
	}
}

func steerStatusKey(item daemon.SteerStatus) string {
	return item.RepositoryID + "\x00" + item.Actor + "\x00" + item.Harness + "\x00" + item.Session
}

func sameSteerStatuses(left, right []daemon.SteerStatus) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !reflect.DeepEqual(left[i], right[i]) {
			return false
		}
	}
	return true
}

func registrationStartupStatuses(registrations []state.SteerRegistration, observationCode string) []daemon.SteerStatus {
	items := make([]daemon.SteerStatus, 0, len(registrations))
	for _, registration := range registrations {
		status := daemon.SteerStatus{
			RepositoryID: registration.RepositoryID, RepositoryName: registration.RepositoryName,
			Role: registration.Role, Actor: registration.Actor, Harness: registration.Harness, Session: registration.SessionID,
			State: "checking",
		}
		if observationCode != "" {
			status.State = "degraded"
			status.Code = observationCode
		}
		items = append(items, status)
	}
	return items
}

func applySteerStatusPolicy(status *daemon.SteerStatus, policy steerRolePolicy) {
	if status == nil {
		return
	}
	status.EffectiveTicketQueue = policy.TicketQueue
	status.EffectiveTicketTags = append([]string(nil), policy.QueueFilters.Tags...)
	status.EffectiveReviewSkipTags = append([]string(nil), policy.QueueFilters.WithoutTags...)
}

type steerClient interface {
	ActiveClaims(context.Context, string, int) (ticketclient.ListResult, error)
	ReadyFrontier(context.Context, string, ticketclient.QueueFilters, int) (ticketclient.ListResult, error)
	Close() error
}

type steerDirtySnapshot struct {
	all          bool
	repositories map[string]struct{}
}

// steerRepositoryRetry tracks only registrations whose authoritative Ticket
// observations have not recovered. The registration loop uses these deadlines
// to re-dirty failed repositories without polling healthy repositories.
type steerRepositoryRetry struct {
	failures      int
	due           time.Time
	registrations map[string]struct{}
}

type steerRetrySchedule struct {
	repositories map[string]*steerRepositoryRetry
}

func newSteerRetrySchedule() *steerRetrySchedule {
	return &steerRetrySchedule{repositories: make(map[string]*steerRepositoryRetry)}
}

func (schedule *steerRetrySchedule) Due(now time.Time) steerDirtySnapshot {
	due := steerDirtySnapshot{repositories: make(map[string]struct{})}
	if schedule == nil {
		return due
	}
	for repositoryID, retry := range schedule.repositories {
		if !retry.due.After(now) {
			due.repositories[repositoryID] = struct{}{}
		}
	}
	return due
}

func (schedule *steerRetrySchedule) Reconcile(work steerDirtySnapshot, registrations []state.SteerRegistration, failed, observed map[string]bool, now time.Time) {
	if schedule == nil || work.empty() {
		return
	}
	workRepositories := work.repositories
	if work.all {
		workRepositories = make(map[string]struct{}, len(registrations)+len(schedule.repositories))
		for _, registration := range registrations {
			workRepositories[registration.RepositoryID] = struct{}{}
		}
		for repositoryID := range schedule.repositories {
			workRepositories[repositoryID] = struct{}{}
		}
	}
	for repositoryID := range workRepositories {
		retry := schedule.repositories[repositoryID]
		current := make(map[string]struct{})
		for _, registration := range registrations {
			if registration.RepositoryID == repositoryID {
				current[steerRegistrationKey(registration)] = struct{}{}
			}
		}
		if retry == nil {
			retry = &steerRepositoryRetry{registrations: make(map[string]struct{})}
			schedule.repositories[repositoryID] = retry
		}
		for key := range retry.registrations {
			if _, found := current[key]; !found {
				delete(retry.registrations, key)
			}
		}
		for key := range current {
			if failed[key] {
				retry.registrations[key] = struct{}{}
			} else if observed[key] {
				delete(retry.registrations, key)
			}
		}
		if len(retry.registrations) == 0 {
			delete(schedule.repositories, repositoryID)
			continue
		}
		retry.failures++
		delay := steerRetryDelay(retry.failures)
		retry.due = now.Add(delay)
	}
}

func steerRetryDelay(failures int) time.Duration {
	delay := time.Second
	for count := 1; count < failures && delay < 15*time.Second; count++ {
		delay *= 2
	}
	if delay > 15*time.Second {
		return 15 * time.Second
	}
	return delay
}

func (snapshot steerDirtySnapshot) empty() bool {
	return !snapshot.all && len(snapshot.repositories) == 0
}

func (snapshot steerDirtySnapshot) includes(repositoryID string) bool {
	if snapshot.all {
		return true
	}
	_, ok := snapshot.repositories[repositoryID]
	return ok
}

// steerDirtySet coalesces reconciliation reasons without blocking producers.
// Drain swaps the pending set under the same lock used by Mark, so marks that
// arrive while reconciliation is running belong to the next snapshot.
type steerDirtySet struct {
	mu           sync.Mutex
	all          bool
	repositories map[string]struct{}
	wake         chan struct{}
}

func newSteerDirtySet() *steerDirtySet {
	return &steerDirtySet{repositories: make(map[string]struct{}), wake: make(chan struct{}, 1)}
}

func (dirty *steerDirtySet) MarkAll() {
	dirty.mu.Lock()
	dirty.all = true
	dirty.repositories = nil
	dirty.signalLocked()
	dirty.mu.Unlock()
}

func (dirty *steerDirtySet) MarkRepository(repositoryID string) {
	if repositoryID == "" {
		return
	}
	dirty.mu.Lock()
	if !dirty.all {
		if dirty.repositories == nil {
			dirty.repositories = make(map[string]struct{})
		}
		dirty.repositories[repositoryID] = struct{}{}
	}
	dirty.signalLocked()
	dirty.mu.Unlock()
}

func (dirty *steerDirtySet) Drain() steerDirtySnapshot {
	dirty.mu.Lock()
	snapshot := steerDirtySnapshot{all: dirty.all, repositories: dirty.repositories}
	dirty.all = false
	dirty.repositories = make(map[string]struct{})
	select {
	case <-dirty.wake:
	default:
	}
	dirty.mu.Unlock()
	return snapshot
}

func (dirty *steerDirtySet) Wake() <-chan struct{} { return dirty.wake }

func (dirty *steerDirtySet) signalLocked() {
	select {
	case dirty.wake <- struct{}{}:
	default:
	}
}

type steerClientFactory func(state.SteerRegistration) (steerClient, error)
type steerRuntimePersistence interface {
	Snapshot(context.Context) (state.SteerRuntimeSnapshot, error)
	Reconcile(context.Context, []state.SteerRegistration) error
	Update(context.Context, state.SteerRegistration, string, string) (bool, error)
	CompleteDelivery(context.Context, state.SteerRegistration, string, bool, bool) (bool, error)
	ConfirmNoActiveClaim(context.Context, state.SteerRegistration) (bool, error)
}

type steerNotificationPlan struct {
	action          orc.Action
	previousState   string
	send            bool
	workBearing     bool
	bootstrap       bool
	recovery        bool
	ticket          string
	message         string
	completionState string
}

func observeSteerActiveTicket(ctx context.Context, client steerClient, policy steerRolePolicy) (string, error) {
	return boundedActorActive(ctx, client)
}

func observeSteerReadyTicket(ctx context.Context, client steerClient, policy steerRolePolicy, stateName, activeTicket string) (string, error) {
	if activeTicket != "" || (stateName != string(orc.SteerIdle) && stateName != string(orc.SteerConsumed)) {
		return "", nil
	}
	return boundedReady(ctx, client, policy.TicketQueue, policy.QueueFilters)
}

func planSteerNotification(policy steerRolePolicy, stateName string, activeTicket, readyTicket string, recoveryPending, bootstrapPending bool) steerNotificationPlan {
	active := activeTicket != ""
	ready := readyTicket != ""
	recovery := recoveryPending && active && stateName == string(orc.SteerIdle)
	bootstrap := bootstrapPending && stateName == string(orc.SteerIdle)
	action := orc.AdvanceSteerDelivery(orc.SteerDeliveryState(stateName), orc.Evidence{Active: active, Ready: ready})
	if recovery {
		action = orc.Action{Next: orc.SteerSending, Queue: true}
	}

	plan := steerNotificationPlan{
		action:        action,
		previousState: stateName,
		send:          action.Queue || recovery || bootstrap,
		workBearing:   ready,
		bootstrap:     bootstrap,
		recovery:      recovery,
		ticket:        readyTicket,
	}
	if plan.ticket == "" {
		plan.ticket = activeTicket
	}
	if !plan.send {
		return plan
	}
	messageParts := make([]string, 0, 3)
	if bootstrap {
		messageParts = append(messageParts, steerSessionBootstrapPrompt)
	}
	if recovery {
		messageParts = append(messageParts, steerClaimRecoveryPrompt)
	}
	if plan.workBearing {
		messageParts = append(messageParts, policy.NudgePrompt)
		if completionPrompt := steerReviewCompletionPrompt(policy); completionPrompt != "" {
			messageParts = append(messageParts, completionPrompt)
		}
	}
	plan.message = strings.Join(messageParts, "\n\n")
	plan.completionState = string(orc.SteerQueued)
	if bootstrap && !plan.workBearing && !recovery {
		plan.completionState = string(orc.SteerIdle)
	}
	return plan
}

func indexSteerDeliveries(snapshot state.SteerRuntimeSnapshot) map[string]state.SteerDelivery {
	deliveries := make(map[string]state.SteerDelivery, len(snapshot.Deliveries))
	for _, delivery := range snapshot.Deliveries {
		deliveries[steerRegistrationKey(state.SteerRegistration{
			RepositoryID:   delivery.RepositoryID,
			Actor:          delivery.Actor,
			RegistrationID: delivery.RegistrationID,
		})] = delivery
	}
	return deliveries
}

type steerDeliveryResult struct {
	state string
	code  string
}

func sendSteerNotification(ctx context.Context, stateDir string, reg state.SteerRegistration, plan steerNotificationPlan, router *steertransport.Router, persistence steerRuntimePersistence, statuses *dynamicSteerStatus, writeFailures, lastPersistedStates map[string]string) steerDeliveryResult {
	key := steerRegistrationKey(reg)
	result := steerDeliveryResult{state: string(plan.action.Next)}
	current, err := persistence.Update(ctx, reg, string(orc.SteerSending), "")
	if err != nil {
		writeFailures[key] = "runtime_state_write_failed"
		result.state = plan.previousState
		result.code = "runtime_state_write_failed"
		return result
	}
	if !current {
		delete(writeFailures, key)
		delete(lastPersistedStates, key)
		result.state = plan.previousState
		result.code = "registration_replaced"
		return result
	}
	lastPersistedStates[key] = string(orc.SteerSending)
	result.state = string(orc.SteerSending)
	statuses.publishDelivery(reg, result.state, plan.ticket)

	queueCtx, queueCancel := context.WithTimeout(ctx, steerOperationTimeout)
	queueErr := router.Deliver(queueCtx, stateDir, reg, steertransport.Message{Kind: steertransport.MessageSteer, Text: plan.message})
	queueCancel()
	if queueErr == nil {
		statuses.publishDelivery(reg, plan.completionState, plan.ticket)
		current, err = persistence.CompleteDelivery(ctx, reg, plan.completionState, plan.bootstrap, plan.recovery)
		if err != nil {
			writeFailures[key] = "runtime_state_write_failed"
			result.code = "runtime_state_write_failed"
		} else if current {
			delete(writeFailures, key)
			lastPersistedStates[key] = plan.completionState
			result.state = plan.completionState
		} else {
			delete(writeFailures, key)
			delete(lastPersistedStates, key)
			result.code = "registration_replaced"
		}
		return result
	}
	if steertransport.IsRejected(queueErr) {
		current, err = persistence.Update(ctx, reg, string(orc.SteerDegraded), "queue_rejected")
		if err != nil {
			writeFailures[key] = "runtime_state_write_failed"
			result.code = "runtime_state_write_failed"
		} else if current {
			delete(writeFailures, key)
			lastPersistedStates[key] = string(orc.SteerDegraded)
			result.state = string(orc.SteerDegraded)
			result.code = "queue_rejected"
		} else {
			delete(writeFailures, key)
			delete(lastPersistedStates, key)
			result.code = "registration_replaced"
		}
		return result
	}
	current, err = persistence.Update(ctx, reg, string(orc.SteerSending), "queue_uncertain")
	if err != nil {
		writeFailures[key] = "runtime_state_write_failed"
		result.code = "runtime_state_write_failed"
	} else if current {
		delete(writeFailures, key)
		lastPersistedStates[key] = string(orc.SteerSending)
		result.code = "queue_uncertain"
	} else {
		delete(writeFailures, key)
		delete(lastPersistedStates, key)
		result.code = "registration_replaced"
	}
	return result
}

func runDynamicSteer(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus) {
	runDynamicSteerWithPersistence(ctx, stateDir, policies, runtime, clients, router, statuses, nil)
}

func runDynamicSteerWithPersistence(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence) {
	runDynamicSteerWithObserver(ctx, stateDir, policies, runtime, clients, router, statuses, persistence, newRegistrationObserver(state.NewRegistrationStore(stateDir)))
}

func runDynamicSteerWithObserver(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver) {
	runDynamicSteerWithGate(ctx, stateDir, policies, runtime, clients, router, statuses, persistence, observer, nil, nil)
}

func runDynamicSteerWithDirtySet(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, dirty *steerDirtySet) {
	runDynamicSteerWithGate(ctx, stateDir, policies, runtime, clients, router, statuses, persistence, newRegistrationObserver(state.NewRegistrationStore(stateDir)), nil, dirty)
}

func runDynamicSteerWithGate(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, dirty *steerDirtySet) {
	registrationTicker := time.NewTicker(steerPollInterval)
	defer registrationTicker.Stop()
	runDynamicSteerWithSchedule(ctx, stateDir, policies, runtime, clients, router, statuses, persistence, observer, gate, dirty, registrationTicker.C)
}

func runDynamicSteerWithPolicyStoreGate(ctx context.Context, stateDir string, policies *steerPolicyStore, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, dirty *steerDirtySet) {
	registrationTicker := time.NewTicker(steerPollInterval)
	defer registrationTicker.Stop()
	runDynamicSteerWithPolicyStore(ctx, stateDir, policies, runtime, clients, router, statuses, persistence, observer, gate, dirty, registrationTicker.C, nil)
}

func runDynamicSteerWithSchedule(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, dirty *steerDirtySet, registrationTicks <-chan time.Time) {
	runDynamicSteerWithElapsedSchedule(ctx, stateDir, policies, runtime, clients, router, statuses, persistence, observer, gate, dirty, registrationTicks, nil)
}

// runDynamicSteerWithElapsedSchedule has a separate elapsed-time input so
// tests can advance the former full-sweep interval without a wall-clock wait.
// Production has no elapsed-time work trigger and passes nil.
func runDynamicSteerWithElapsedSchedule(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, dirty *steerDirtySet, registrationTicks, elapsedTicks <-chan time.Time) {
	runDynamicSteerWithPolicyStore(ctx, stateDir, newSteerPolicyStore(policies), runtime, clients, router, statuses, persistence, observer, gate, dirty, registrationTicks, elapsedTicks)
}

func runDynamicSteerWithPolicyStore(ctx context.Context, stateDir string, policies *steerPolicyStore, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, router *steertransport.Router, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, dirty *steerDirtySet, registrationTicks, elapsedTicks <-chan time.Time) {
	if ctx == nil || policies == nil || len(policies.Snapshot().roles) == 0 {
		return
	}
	if dirty == nil {
		dirty = newSteerDirtySet()
	}
	if clients == nil {
		clients = func(reg state.SteerRegistration) (steerClient, error) {
			return ticketclient.NewWithWorkingDirAndTarget(reg.Actor, reg.RepositoryPath, ticketclient.Target{Repository: reg.RepositoryPath})
		}
	}
	if observer == nil {
		observer = newRegistrationObserver(state.NewRegistrationStore(stateDir))
	}
	if router == nil {
		var err error
		router, err = newDefaultSteerTransportRouter()
		if err != nil {
			statuses.replace(steerFailureStatuses(observer.lastRegistrations(), "transport_unavailable", policies.Snapshot()))
			return
		}
	}
	if persistence == nil {
		persistence = state.NewSteerRuntimeStore(stateDir)
	}
	connections := map[string]steerClient{}
	writeFailures := map[string]string{}
	lastPersistedStates := map[string]string{}
	latestObservation := registrationObservation{}
	haveObservation := false
	forceFullAfterRecovery := false
	spoolReconciled := false
	lastRegistrations := []state.SteerRegistration{}
	statusCache := map[string]daemon.SteerStatus{}
	retries := newSteerRetrySchedule()
	observeRegistrations := func() (bool, steerDirtySnapshot) {
		readCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		observation := observer.Observe(readCtx)
		cancel()
		if observation.Code != "" {
			closeSteerClients(connections)
			items := registrationFailureStatuses(observation, policies.Snapshot())
			statusCache = indexSteerStatuses(observation.Registrations, items)
			statuses.replace(items)
			forceFullAfterRecovery = true
			return false, steerDirtySnapshot{}
		}
		if !spoolReconciled {
			reconcileCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
			err := reconcileSteerSpoolEndpoints(reconcileCtx, observer.store, stateDir)
			cancel()
			if err != nil {
				items := steerFailureStatuses(observation.Registrations, "spool_reconciliation_failed", policies.Snapshot())
				statusCache = indexSteerStatuses(observation.Registrations, items)
				statuses.replace(items)
				return false, steerDirtySnapshot{}
			}
			spoolReconciled = true
		}
		changed := steerDirtySnapshot{repositories: make(map[string]struct{})}
		if !haveObservation {
			changed.all = true
			haveObservation = true
		} else {
			for repositoryID := range changedSteerRepositories(lastRegistrations, observation.Registrations) {
				changed.repositories[repositoryID] = struct{}{}
			}
		}
		if forceFullAfterRecovery {
			changed.all = true
			changed.repositories = nil
			forceFullAfterRecovery = false
		}
		latestObservation = observation
		lastRegistrations = append(lastRegistrations[:0], observation.Registrations...)
		return true, changed
	}
	tick := func(work steerDirtySnapshot, now time.Time) {
		if !haveObservation || work.empty() {
			return
		}
		policySnapshot := policies.Snapshot()
		observation := latestObservation
		if err := persistence.Reconcile(ctx, observation.Registrations); err != nil {
			closeSteerClients(connections)
			items := steerFailureStatuses(observation.Registrations, "runtime_state_unavailable", policies.Snapshot())
			for i := range items {
				reg := observation.Registrations
				if i < len(reg) {
					applySteerWriteFailure(&items[i], reg[i], writeFailures, lastPersistedStates)
				}
			}
			statusCache = indexSteerStatuses(observation.Registrations, items)
			statuses.replace(items)
			return
		}
		stored, snapshotErr := persistence.Snapshot(ctx)
		storedDeliveries := map[string]state.SteerDelivery{}
		if snapshotErr == nil {
			storedDeliveries = indexSteerDeliveries(stored)
		}
		activeKeys := make(map[string]bool, len(observation.Registrations))
		currentRegistrationKeys := make(map[string]bool, len(observation.Registrations))
		for _, reg := range observation.Registrations {
			currentRegistrationKeys[steerRegistrationKey(reg)] = true
		}
		for key := range writeFailures {
			if !currentRegistrationKeys[key] {
				delete(writeFailures, key)
			}
		}
		for key := range lastPersistedStates {
			if !currentRegistrationKeys[key] {
				delete(lastPersistedStates, key)
			}
		}
		currentStatus := make([]daemon.SteerStatus, 0, len(observation.Registrations))
		failedTicketObservations := make(map[string]bool)
		successfulTicketObservations := make(map[string]bool)
		for _, reg := range observation.Registrations {
			regKey := steerRegistrationKey(reg)
			policy, ok := policySnapshot.roles[reg.Role]
			if !ok {
				status := daemon.SteerStatus{RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName, Role: reg.Role, Actor: reg.Actor, Harness: reg.Harness, Session: reg.SessionID, State: "degraded", Code: "unknown_role"}
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			status := daemon.SteerStatus{RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName, Role: reg.Role, Actor: reg.Actor, Harness: reg.Harness, Session: reg.SessionID}
			applySteerStatusPolicy(&status, policy)
			key := regKey
			activeKeys[key] = true
			if !work.includes(reg.RepositoryID) {
				if status, found := statusCache[key]; found {
					applySteerStatusPolicy(&status, policy)
					currentStatus = append(currentStatus, status)
				} else {
					status.State = "checking"
					currentStatus = append(currentStatus, status)
				}
				continue
			}
			if managedOwner(runtime, reg) {
				status.State = "conflict"
				status.Code = "managed_owner"
				status.ManagedOwner = managedOwnerName(runtime, reg)
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			if snapshotErr != nil {
				status.State = lastPersistedStates[key]
				if status.State == "" {
					status.State = "degraded"
				}
				status.Code = "runtime_state_unavailable"
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			stateName := string(orc.SteerIdle)
			recoveryPending := false
			bootstrapPending := false
			if delivery, found := storedDeliveries[key]; found {
				stateName = delivery.State
				status.Code = delivery.Code
				recoveryPending = delivery.RecoveryPending
				bootstrapPending = delivery.BootstrapPending
			}
			lastPersistedStates[key] = stateName
			if code := writeFailures[key]; code != "" {
				status.Code = code
			}
			var err error
			client := connections[key]
			if client == nil {
				client, err = clients(reg)
				if err != nil {
					failedTicketObservations[regKey] = true
					status.State = stateName
					status.Code = "ticket_client_failed"
					applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
					currentStatus = append(currentStatus, status)
					continue
				}
				connections[key] = client
			}
			var loopStore *state.Store
			var loopReader orc.TicketLoopReader
			var loopHolder orc.TicketLoopHolder
			if policy.MaxBounces > 0 && policy.ContainmentActor != "" {
				if reader, ok := client.(orc.TicketLoopReader); ok {
					loopStore = state.NewForRepository(stateDir, reg.RepositoryID)
					loopReader = reader
					loopHolder = containmentTicketClient{actor: policy.ContainmentActor, workingDir: reg.RepositoryPath, target: ticketclient.Target{Repository: reg.RepositoryPath}}
				}
			}
			activeTicket, observeErr := observeSteerActiveTicket(ctx, client, policy)
			if observeErr != nil {
				if steerClientUnusable(observeErr) {
					discardSteerClient(connections, key)
				}
				failedTicketObservations[regKey] = true
				status.State = stateName
				status.Code = "ticket_observation_failed"
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			status.Ticket = activeTicket
			active := activeTicket != ""
			if loopStore != nil {
				if err := orc.ReconcileTicketLoops(ctx, loopStore, loopReader, loopHolder); err != nil {
					status.State = stateName
					status.Code = "ticket_loop_reconciliation_failed"
					currentStatus = append(currentStatus, status)
					continue
				}
			}
			if active && loopStore != nil {
				activeInfo, readErr := loopReader.Show(ctx, activeTicket)
				if readErr != nil {
					failedTicketObservations[regKey] = true
					status.State = stateName
					status.Code = "ticket_observation_failed"
					currentStatus = append(currentStatus, status)
					continue
				}
				activeInfo.State = ticketclient.NormalizeLifecycleState(activeInfo.State)
				if activeInfo.State == "open" || activeInfo.State == "review" {
					loop, exists, loopErr := loopStore.TicketLoop(ctx, activeTicket)
					if loopErr == nil && exists && loop.DispatchPending {
						loop, _, loopErr = loopStore.RecordSteerClaim(ctx, activeTicket, reg.Actor, activeInfo.State)
					}
					if loopErr != nil {
						status.State = stateName
						status.Code = "ticket_loop_state_failed"
						currentStatus = append(currentStatus, status)
						continue
					}
					if exists && loop.Phase != state.TicketLoopActive {
						status.State = stateName
						status.Code = "ticket_containment_pending"
						currentStatus = append(currentStatus, status)
						continue
					}
				}
			}
			if !active && recoveryPending {
				current := false
				allowed, policyCurrent, persistErr := withSteerPolicyDispatch(gate, policies, policySnapshot.generation, func() error {
					var err error
					current, err = persistence.ConfirmNoActiveClaim(ctx, reg)
					return err
				})
				if persistErr != nil {
					writeFailures[regKey] = "runtime_state_write_failed"
					status.State = stateName
					status.Code = "runtime_state_write_failed"
					currentStatus = append(currentStatus, status)
					continue
				}
				if !allowed {
					status.State = stateName
					status.Code = steerDispatchSuppressedCode(gate)
				} else if !policyCurrent {
					dirty.MarkRepository(reg.RepositoryID)
					status.State = stateName
					status.Code = "policy_changed"
					currentStatus = append(currentStatus, status)
					continue
				} else if !current {
					delete(writeFailures, regKey)
					delete(lastPersistedStates, regKey)
					status.State = stateName
					status.Code = "registration_replaced"
					currentStatus = append(currentStatus, status)
					continue
				} else {
					delete(writeFailures, regKey)
					recoveryPending = false
				}
			}
			readyTicket, observeErr := observeSteerReadyTicket(ctx, client, policy, stateName, activeTicket)
			if observeErr != nil {
				if steerClientUnusable(observeErr) {
					discardSteerClient(connections, key)
				}
				failedTicketObservations[regKey] = true
				status.State = stateName
				status.Code = "ticket_observation_failed"
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			successfulTicketObservations[regKey] = true
			plan := planSteerNotification(policy, stateName, activeTicket, readyTicket, recoveryPending, bootstrapPending)
			action := plan.action
			status.State = string(action.Next)
			if action.Code != "" {
				status.Code = action.Code
			}
			// A pending handoff gets one queue attempt per explicit join signal.
			// Failed/uncertain attempts stay pending but are not auto-retried on
			// later scheduler ticks; a fresh join resets state to none and signals
			// that the session is healthy enough to try again.
			if plan.send {
				var result steerDeliveryResult
				registrationCurrent := false
				allowed, policyCurrent, sendErr := withSteerPolicyDispatch(gate, policies, policySnapshot.generation, func() error {
					currentCtx, currentCancel := context.WithTimeout(ctx, steerOperationTimeout)
					var currentErr error
					registrationCurrent, currentErr = currentSteerRegistration(currentCtx, observer.store, reg)
					currentCancel()
					if currentErr != nil {
						return currentErr
					}
					if !registrationCurrent {
						result = steerDeliveryResult{state: stateName, code: "registration_replaced"}
						return nil
					}
					if plan.workBearing && loopStore != nil && plan.ticket != "" {
						loop, _, loopErr := loopStore.Participate(ctx, plan.ticket, policy.MaxBounces)
						if loopErr != nil {
							return loopErr
						}
						if loop.Phase != state.TicketLoopActive {
							if _, _, reconcileErr := orc.ReconcileTicketLoop(ctx, loopStore, loopReader, loopHolder, plan.ticket); reconcileErr != nil {
								return reconcileErr
							}
							result = steerDeliveryResult{state: stateName, code: "ticket_containment_pending"}
							return nil
						}
						if _, loopErr := loopStore.RecordDispatch(ctx, plan.ticket); loopErr != nil {
							return loopErr
						}
					}
					result = sendSteerNotification(ctx, stateDir, reg, plan, router, persistence, statuses, writeFailures, lastPersistedStates)
					if plan.workBearing && loopStore != nil && plan.ticket != "" && result.state != plan.completionState && result.code != "queue_uncertain" {
						if clearErr := loopStore.ClearDispatch(ctx, plan.ticket); clearErr != nil {
							return clearErr
						}
					}
					return nil
				})
				if sendErr != nil {
					status.State = stateName
					status.Code = "registration_state_unavailable"
				} else if !allowed {
					status.State = stateName
					status.Code = steerDispatchSuppressedCode(gate)
				} else if !policyCurrent {
					dirty.MarkRepository(reg.RepositoryID)
					status.State = stateName
					status.Code = "policy_changed"
				} else if !registrationCurrent {
					status.State = stateName
					status.Code = "registration_replaced"
				} else {
					status.State = result.state
					status.Code = result.code
				}
			} else if action.Next != orc.SteerDeliveryState(stateName) || action.Retire {
				current := false
				allowed, policyCurrent, persistErr := withSteerPolicyDispatch(gate, policies, policySnapshot.generation, func() error {
					var err error
					current, err = persistence.Update(ctx, reg, string(action.Next), action.Code)
					return err
				})
				if persistErr != nil {
					writeFailures[steerRegistrationKey(reg)] = "runtime_state_write_failed"
					status.State = stateName
					status.Code = "runtime_state_write_failed"
				} else if !allowed {
					status.State = stateName
					status.Code = steerDispatchSuppressedCode(gate)
				} else if !policyCurrent {
					dirty.MarkRepository(reg.RepositoryID)
					status.State = stateName
					status.Code = "policy_changed"
				} else if !current {
					delete(writeFailures, regKey)
					delete(lastPersistedStates, regKey)
					status.State = stateName
					status.Code = "registration_replaced"
				} else {
					delete(writeFailures, regKey)
					lastPersistedStates[regKey] = string(action.Next)
				}
			}
			if code := writeFailures[steerRegistrationKey(reg)]; code != "" {
				status.Code = code
			}
			currentStatus = append(currentStatus, status)
		}
		reconciledAt := time.Now()
		if now.After(reconciledAt) {
			reconciledAt = now
		}
		retries.Reconcile(work, observation.Registrations, failedTicketObservations, successfulTicketObservations, reconciledAt)
		for key, client := range connections {
			if !activeKeys[key] {
				_ = client.Close()
				delete(connections, key)
			}
		}
		statuses.replace(currentStatus)
		statusCache = indexSteerStatuses(observation.Registrations, currentStatus)
	}
	defer func() {
		for _, client := range connections {
			_ = client.Close()
		}
	}()
	pending := dirty.Drain()
	if observed, changes := observeRegistrations(); observed {
		tick(mergeSteerDirtySnapshots(pending, changes), time.Now())
	}
	for {
		select {
		case <-ctx.Done():
			return
		case tickAt := <-registrationTicks:
			now := time.Now()
			if tickAt.After(now) {
				now = tickAt
			}
			pending := dirty.Drain()
			// Registration polling stays cheap; only repositories with due Ticket
			// observation failures are added to this reconciliation.
			pending = mergeSteerDirtySnapshots(pending, retries.Due(now))
			if observed, changes := observeRegistrations(); observed {
				tick(mergeSteerDirtySnapshots(pending, changes), now)
			}
		case <-elapsedTicks:
			// Time passing alone is not authoritative Ticket evidence and must
			// not trigger ActiveClaims or ReadyFrontier reads.
		case <-dirty.Wake():
			pending := dirty.Drain()
			if observed, changes := observeRegistrations(); observed {
				tick(mergeSteerDirtySnapshots(pending, changes), time.Now())
			}
		}
	}
}

func withSteerDispatch(gate *dispatchGate, operation func() error) (bool, error) {
	if gate == nil {
		return true, operation()
	}
	return gate.WithDispatch(operation)
}

func withSteerPolicyDispatch(gate *dispatchGate, policies *steerPolicyStore, generation uint64, operation func() error) (bool, bool, error) {
	policyCurrent := false
	allowed, err := withSteerDispatch(gate, func() error {
		var policyErr error
		policyCurrent, policyErr = policies.WithGeneration(generation, operation)
		return policyErr
	})
	return allowed, policyCurrent, err
}

func steerDispatchSuppressedCode(gate *dispatchGate) string {
	if gate != nil && gate.Mode() == state.DaemonAborted {
		return "daemon_aborted"
	}
	return "daemon_paused"
}

func applySteerWriteFailure(status *daemon.SteerStatus, reg state.SteerRegistration, failures, persisted map[string]string) {
	key := steerRegistrationKey(reg)
	if failures[key] == "" {
		return
	}
	if status.State == "conflict" && status.Code == "managed_owner" {
		status.PersistenceCode = failures[key]
		return
	}
	status.Code = failures[key]
	if stateName := persisted[key]; stateName != "" {
		status.State = stateName
	}
}

func steerRegistrationKey(reg state.SteerRegistration) string {
	return fmt.Sprintf("%s\x00%s\x00%s", reg.RepositoryID, reg.Actor, reg.RegistrationID)
}

func changedSteerRepositories(previous, current []state.SteerRegistration) map[string]struct{} {
	previousByKey := make(map[string]state.SteerRegistration, len(previous))
	currentByKey := make(map[string]state.SteerRegistration, len(current))
	for _, registration := range previous {
		previousByKey[registrationObservationKey(registration)] = registration
	}
	for _, registration := range current {
		currentByKey[registrationObservationKey(registration)] = registration
	}
	changed := make(map[string]struct{})
	for key, registration := range previousByKey {
		if next, found := currentByKey[key]; !found || !sameSteerRegistration(next, registration) {
			changed[registration.RepositoryID] = struct{}{}
			if found {
				changed[next.RepositoryID] = struct{}{}
			}
		}
	}
	for key, registration := range currentByKey {
		if previous, found := previousByKey[key]; !found || !sameSteerRegistration(previous, registration) {
			changed[registration.RepositoryID] = struct{}{}
		}
	}
	delete(changed, "")
	return changed
}

func mergeSteerDirtySnapshots(left, right steerDirtySnapshot) steerDirtySnapshot {
	if left.all || right.all {
		return steerDirtySnapshot{all: true}
	}
	merged := steerDirtySnapshot{repositories: make(map[string]struct{}, len(left.repositories)+len(right.repositories))}
	for repositoryID := range left.repositories {
		merged.repositories[repositoryID] = struct{}{}
	}
	for repositoryID := range right.repositories {
		merged.repositories[repositoryID] = struct{}{}
	}
	return merged
}

func registrationObservationKey(registration state.SteerRegistration) string {
	return registration.RepositoryID + "\x00" + registration.Actor
}

func indexSteerStatuses(registrations []state.SteerRegistration, statuses []daemon.SteerStatus) map[string]daemon.SteerStatus {
	indexed := make(map[string]daemon.SteerStatus, len(registrations))
	for i, registration := range registrations {
		if i < len(statuses) {
			indexed[steerRegistrationKey(registration)] = statuses[i]
		}
	}
	return indexed
}

func steerFailureStatuses(registrations []state.SteerRegistration, code string, policies steerPolicySnapshot) []daemon.SteerStatus {
	items := make([]daemon.SteerStatus, 0, len(registrations))
	for _, reg := range registrations {
		status := daemon.SteerStatus{RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName, Role: reg.Role, Actor: reg.Actor, Harness: reg.Harness, Session: reg.SessionID, State: "degraded", Code: code}
		if policy, ok := policies.roles[reg.Role]; ok {
			applySteerStatusPolicy(&status, policy)
		}
		items = append(items, status)
	}
	if len(items) == 0 {
		items = append(items, daemon.SteerStatus{State: "degraded", Code: code})
	}
	return items
}

func registrationFailureStatuses(observation registrationObservation, policies steerPolicySnapshot) []daemon.SteerStatus {
	return steerFailureStatuses(observation.Registrations, observation.Code, policies)
}

func closeSteerClients(connections map[string]steerClient) {
	for key, client := range connections {
		_ = client.Close()
		delete(connections, key)
	}
}

// Ticket's persistent JSONL client is terminal after a broken transport or a
// canceled operation. Drop it so the next scheduler tick can establish a new
// process after a transient repository edit or Ticket restart.
func steerClientUnusable(err error) bool {
	return errors.Is(err, ticketclient.ErrTransport) || errors.Is(err, ticketclient.ErrProtocol) || errors.Is(err, context.DeadlineExceeded)
}

func discardSteerClient(connections map[string]steerClient, key string) {
	if client := connections[key]; client != nil {
		_ = client.Close()
		delete(connections, key)
	}
}

// A Ticket claim belongs to its actor, independent of Orc's current role. Check
// both supported queues so changing roles cannot hide work claimed in the
// previous role's queue.
func boundedActorActive(parent context.Context, client steerClient) (string, error) {
	ctx, cancel := context.WithTimeout(parent, steerOperationTimeout)
	defer cancel()
	active, err := client.ActiveClaims(ctx, "open", 1)
	if err != nil || len(active.Items) > 0 {
		if len(active.Items) > 0 {
			return active.Items[0].ID, nil
		}
		return "", err
	}
	active, err = client.ActiveClaims(ctx, "review", 1)
	if err != nil || len(active.Items) == 0 {
		return "", err
	}
	return active.Items[0].ID, nil
}

func boundedReady(parent context.Context, client steerClient, queue string, filters ticketclient.QueueFilters) (string, error) {
	ctx, cancel := context.WithTimeout(parent, steerOperationTimeout)
	defer cancel()
	frontier, err := client.ReadyFrontier(ctx, queue, filters, 1)
	if err != nil || len(frontier.Items) == 0 {
		return "", err
	}
	return frontier.Items[0].ID, nil
}

func managedOwner(runtime *supervisor.RuntimeState[supervisor.RunWorker], reg state.SteerRegistration) bool {
	return managedOwnerName(runtime, reg) != ""
}

func managedOwnerName(runtime *supervisor.RuntimeState[supervisor.RunWorker], reg state.SteerRegistration) string {
	if runtime == nil {
		return ""
	}
	states := runtime.Snapshot()
	for _, worker := range runtime.EffectiveWorkers() {
		transition := states[worker.Name]
		if transition.State != supervisor.WorkerRunning && transition.State != supervisor.WorkerStarting {
			continue
		}
		if worker.Config.Actor == reg.Actor && worker.Config.RepositoryIdentity == reg.RepositoryID {
			return worker.Name
		}
	}
	return ""
}
