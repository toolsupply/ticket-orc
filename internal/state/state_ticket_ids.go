package state

import "sort"

// TicketIDs returns every Ticket identifier retained by the snapshot. Keep
// this inventory centralized so cleanup and garbage collection use the same
// state boundary.
func (snapshot Snapshot) TicketIDs() []string {
	unique := make(map[string]struct{})
	add := func(ticket string) {
		if ticket != "" {
			unique[ticket] = struct{}{}
		}
	}
	for _, session := range snapshot.Sessions {
		add(session.Ticket)
	}
	for key := range snapshot.Bounces {
		_, ticket, namespaced := splitBounceKey(key)
		if namespaced {
			add(ticket)
		}
	}
	for _, loop := range snapshot.Loops {
		add(loop.Ticket)
	}
	ids := make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
