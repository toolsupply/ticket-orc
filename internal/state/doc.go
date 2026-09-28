// Package state stores retained managed harness sessions, review-bounce
// counts, and local steer registrations in versioned JSON files protected by a
// cross-process lock. Managed state is repository-scoped; steer registrations
// use Ticket's canonical repository ID and actor as their key.
package state
