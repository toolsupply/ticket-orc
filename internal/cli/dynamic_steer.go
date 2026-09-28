package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/toolsupply/ticket-orc/internal/daemon"
	"github.com/toolsupply/ticket-orc/internal/harness/codex"
	"github.com/toolsupply/ticket-orc/internal/orc"
	"github.com/toolsupply/ticket-orc/internal/state"
	"github.com/toolsupply/ticket-orc/internal/supervisor"
	"github.com/toolsupply/ticket-orc/internal/ticketclient"
)

const steerPollInterval = time.Second
const steerOperationTimeout = 5 * time.Second

const steerClaimRecoveryPrompt = "[ticket-orc] This session replaces the previous session for this Ticket actor. There is already an active Ticket claim. Inspect the actor's current claim and continue it before taking new work. After resolving it, continue processing actionable work for the current actor until none remains."
const steerSessionBootstrapPrompt = "[ticket-orc] This session is registered for Orc-steered Ticket work.\n\nFor Orc-steered work, follow the installed ticket-orc-worker Skill and the Ticket Tasks Skill it references. If those instructions are not currently available or clear in this session, consult them before acting on Orc work. Ticket remains authoritative for work selection, ownership, and lifecycle."

type steerRolePolicy struct {
	TicketQueue      string
	NudgePrompt      string
	ReviewCompletion string
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
		Type: "steer.delivery", Role: reg.Role, Actor: reg.Actor, Session: shortIdentity(reg.ThreadID),
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
		if found && old == item {
			continue
		}
		publish(daemon.Event{Type: "steer.status", Role: item.Role, Actor: item.Actor, Session: shortIdentity(item.Session), State: item.State, Code: item.Code, Ticket: item.Ticket, RepositoryID: item.RepositoryID, RepositoryName: item.RepositoryName})
	}
	for _, old := range previous {
		found := false
		for _, item := range items {
			if item.RepositoryID == old.RepositoryID && item.Actor == old.Actor && item.Session == old.Session {
				found = true
				break
			}
		}
		if !found {
			publish(daemon.Event{Type: "steer.status", Role: old.Role, Actor: old.Actor, Session: shortIdentity(old.Session), State: "left", Code: "registration_removed", RepositoryID: old.RepositoryID, RepositoryName: old.RepositoryName})
		}
	}
}

func steerStatusKey(item daemon.SteerStatus) string {
	return item.RepositoryID + "\x00" + item.Actor + "\x00" + item.Session
}

func sameSteerStatuses(left, right []daemon.SteerStatus) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
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
			Role: registration.Role, Actor: registration.Actor, Session: registration.ThreadID,
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

type steerClient interface {
	ActiveClaims(context.Context, string, []string, int) (ticketclient.ListResult, error)
	ReadyFrontier(context.Context, string, []string, int) (ticketclient.ListResult, error)
	Close() error
}
type steerClientFactory func(state.SteerRegistration) (steerClient, error)
type steerQueue func(context.Context, string, string, string) error
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
	return boundedActorActive(ctx, client, policy.TicketQueue)
}

func observeSteerReadyTicket(ctx context.Context, client steerClient, policy steerRolePolicy, stateName, activeTicket string) (string, error) {
	if activeTicket != "" || (stateName != string(orc.SteerIdle) && stateName != string(orc.SteerConsumed)) {
		return "", nil
	}
	return boundedReady(ctx, client, policy.TicketQueue)
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
		workBearing:   active || ready,
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

func sendSteerNotification(ctx context.Context, reg state.SteerRegistration, plan steerNotificationPlan, queue steerQueue, persistence steerRuntimePersistence, statuses *dynamicSteerStatus, writeFailures, lastPersistedStates map[string]string) steerDeliveryResult {
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
	queueErr := queue(queueCtx, reg.CodexHome, reg.ThreadID, plan.message)
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
	var processErr *codex.ProcessError
	if errors.As(queueErr, &processErr) {
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

func runDynamicSteer(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, queue steerQueue, statuses *dynamicSteerStatus) {
	runDynamicSteerWithPersistence(ctx, stateDir, policies, runtime, clients, queue, statuses, nil)
}

func runDynamicSteerWithPersistence(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, queue steerQueue, statuses *dynamicSteerStatus, persistence steerRuntimePersistence) {
	runDynamicSteerWithObserver(ctx, stateDir, policies, runtime, clients, queue, statuses, persistence, newRegistrationObserver(state.NewRegistrationStore(stateDir)))
}

func runDynamicSteerWithObserver(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, queue steerQueue, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver) {
	runDynamicSteerWithGate(ctx, stateDir, policies, runtime, clients, queue, statuses, persistence, observer, nil, nil)
}

func runDynamicSteerWithGate(ctx context.Context, stateDir string, policies map[string]steerRolePolicy, runtime *supervisor.RuntimeState[supervisor.RunWorker], clients steerClientFactory, queue steerQueue, statuses *dynamicSteerStatus, persistence steerRuntimePersistence, observer *registrationObserver, gate *dispatchGate, wake <-chan struct{}) {
	if ctx == nil || len(policies) == 0 {
		return
	}
	if clients == nil {
		clients = func(reg state.SteerRegistration) (steerClient, error) {
			return ticketclient.NewWithWorkingDirAndTarget(reg.Actor, reg.RepositoryPath, ticketclient.Target{Repository: reg.RepositoryPath})
		}
	}
	if queue == nil {
		queue = func(ctx context.Context, home, thread, message string) error {
			return codex.New().Queue(ctx, home, thread, message)
		}
	}
	if persistence == nil {
		persistence = state.NewSteerRuntimeStore(stateDir)
	}
	connections := map[string]steerClient{}
	writeFailures := map[string]string{}
	lastPersistedStates := map[string]string{}
	tick := func() {
		readCtx, cancel := context.WithTimeout(ctx, steerOperationTimeout)
		observation := observer.Observe(readCtx)
		cancel()
		if observation.Code != "" {
			closeSteerClients(connections)
			statuses.replace(registrationFailureStatuses(observation))
			return
		}
		if err := persistence.Reconcile(ctx, observation.Registrations); err != nil {
			closeSteerClients(connections)
			items := steerFailureStatuses(observation.Registrations, "runtime_state_unavailable")
			for i := range items {
				reg := observation.Registrations
				if i < len(reg) {
					applySteerWriteFailure(&items[i], reg[i], writeFailures, lastPersistedStates)
				}
			}
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
		for _, reg := range observation.Registrations {
			regKey := steerRegistrationKey(reg)
			policy, ok := policies[reg.Role]
			if !ok {
				status := daemon.SteerStatus{RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName, Role: reg.Role, Actor: reg.Actor, Session: reg.ThreadID, State: "degraded", Code: "unknown_role"}
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			key := regKey
			activeKeys[key] = true
			status := daemon.SteerStatus{RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName, Role: reg.Role, Actor: reg.Actor, Session: reg.ThreadID}
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
					status.State = stateName
					status.Code = "ticket_client_failed"
					applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
					currentStatus = append(currentStatus, status)
					continue
				}
				connections[key] = client
			}
			activeTicket, observeErr := observeSteerActiveTicket(ctx, client, policy)
			if observeErr != nil {
				if steerClientUnusable(observeErr) {
					discardSteerClient(connections, key)
				}
				status.State = stateName
				status.Code = "ticket_observation_failed"
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
			status.Ticket = activeTicket
			active := activeTicket != ""
			if !active && recoveryPending {
				current := false
				allowed, persistErr := withSteerDispatch(gate, func() error {
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
				status.State = stateName
				status.Code = "ticket_observation_failed"
				applySteerWriteFailure(&status, reg, writeFailures, lastPersistedStates)
				currentStatus = append(currentStatus, status)
				continue
			}
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
				allowed, sendErr := withSteerDispatch(gate, func() error {
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
					result = sendSteerNotification(ctx, reg, plan, queue, persistence, statuses, writeFailures, lastPersistedStates)
					return nil
				})
				if sendErr != nil {
					status.State = stateName
					status.Code = "registration_state_unavailable"
				} else if !allowed {
					status.State = stateName
					status.Code = steerDispatchSuppressedCode(gate)
				} else if !registrationCurrent {
					status.State = stateName
					status.Code = "registration_replaced"
				} else {
					status.State = result.state
					status.Code = result.code
				}
			} else if action.Next != orc.SteerDeliveryState(stateName) || action.Retire {
				current := false
				allowed, persistErr := withSteerDispatch(gate, func() error {
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
		for key, client := range connections {
			if !activeKeys[key] {
				_ = client.Close()
				delete(connections, key)
			}
		}
		statuses.replace(currentStatus)
	}
	tick()
	ticker := time.NewTicker(steerPollInterval)
	defer ticker.Stop()
	defer func() {
		for _, client := range connections {
			_ = client.Close()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		case <-wake:
			tick()
		}
	}
}

func withSteerDispatch(gate *dispatchGate, operation func() error) (bool, error) {
	if gate == nil {
		return true, operation()
	}
	return gate.WithDispatch(operation)
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

func steerFailureStatuses(registrations []state.SteerRegistration, code string) []daemon.SteerStatus {
	items := make([]daemon.SteerStatus, 0, len(registrations))
	for _, reg := range registrations {
		items = append(items, daemon.SteerStatus{RepositoryID: reg.RepositoryID, RepositoryName: reg.RepositoryName, Role: reg.Role, Actor: reg.Actor, Session: reg.ThreadID, State: "degraded", Code: code})
	}
	if len(items) == 0 {
		items = append(items, daemon.SteerStatus{State: "degraded", Code: code})
	}
	return items
}

func registrationFailureStatuses(observation registrationObservation) []daemon.SteerStatus {
	return steerFailureStatuses(observation.Registrations, observation.Code)
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
func boundedActorActive(parent context.Context, client steerClient, currentQueue string) (string, error) {
	if currentQueue != "open" && currentQueue != "review" {
		return "", fmt.Errorf("ticket queue must be open or review")
	}
	ctx, cancel := context.WithTimeout(parent, steerOperationTimeout)
	defer cancel()
	active, err := client.ActiveClaims(ctx, currentQueue, nil, 1)
	if err != nil || len(active.Items) > 0 {
		if len(active.Items) > 0 {
			return active.Items[0].ID, nil
		}
		return "", err
	}
	otherQueue := "open"
	if currentQueue == "open" {
		otherQueue = "review"
	}
	active, err = client.ActiveClaims(ctx, otherQueue, nil, 1)
	if err != nil || len(active.Items) == 0 {
		return "", err
	}
	return active.Items[0].ID, nil
}

func boundedReady(parent context.Context, client steerClient, queue string) (string, error) {
	ctx, cancel := context.WithTimeout(parent, steerOperationTimeout)
	defer cancel()
	frontier, err := client.ReadyFrontier(ctx, queue, nil, 1)
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
