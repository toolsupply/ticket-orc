# Supervisor lock ordering

Lifecycle operations follow this order:

1. The supervisor-wide lifecycle mutex serializes reload, start, stop, pause,
   resume, restart, group operations, and shutdown.
2. A per-worker transition mutex serializes a worker's state transition while
   the global mutex is held.
3. The manager state mutex protects the worker, child, startup-attempt, and
   desired-state maps. It is held only for in-memory reads or updates.

The manager state mutex must never be held while acquiring a transition mutex,
starting or stopping a process, probing Ticket, waiting for readiness or exit,
or invoking event/transition callbacks. Look up the worker transition mutex
under the state mutex, release the state mutex, then acquire the transition
mutex. The global lifecycle mutex may remain held across external work to keep
conflicting lifecycle transactions ordered, but the smaller state mutex may
not. Repository observer and runtime-state mutexes protect their own data and
must not call back into the manager while still held.

The lifecycle manager owns desired worker state, managed-child registration,
transition serialization, readiness waiting, and exactly-once exit
reconciliation. `internal/cli` supplies process-launch and terminal-rendering
adapters; it does not mutate lifecycle maps or decide whether a child result is
still current.
