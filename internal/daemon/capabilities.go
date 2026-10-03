package daemon

// Capabilities describes the operations wired into one running daemon. It is
// computed from the callbacks used by the HTTP handlers rather than from
// operations merely supported by the binary.
type Capabilities struct {
	Events        bool                   `json:"events"`
	DaemonActions []string               `json:"daemon_actions"`
	WorkerActions []string               `json:"worker_actions"`
	GroupActions  []string               `json:"group_actions"`
	Repositories  RepositoryCapabilities `json:"repositories"`
}

// RepositoryCapabilities describes the repository HTTP surface and its
// request/response bounds.
type RepositoryCapabilities struct {
	Inventory             bool     `json:"inventory"`
	TicketList            bool     `json:"ticket_list"`
	TicketDetail          bool     `json:"ticket_detail"`
	TicketCreate          bool     `json:"ticket_create"`
	TicketUpdate          bool     `json:"ticket_update"`
	TicketActions         []string `json:"ticket_actions"`
	MaxPageSize           int      `json:"max_page_size"`
	MaxSearchBytes        int      `json:"max_search_bytes"`
	TicketBodyBudgetBytes int      `json:"ticket_body_budget_bytes,omitempty"`
}

func (s *Server) capabilities() Capabilities {
	gateway := s.config.RepositoryGateway
	capabilities := Capabilities{
		Events:        s.events != nil,
		DaemonActions: []string{},
		WorkerActions: []string{},
		GroupActions:  []string{},
		Repositories: RepositoryCapabilities{
			Inventory:      true,
			TicketList:     gateway.ListTickets != nil,
			TicketDetail:   gateway.GetTicket != nil,
			TicketCreate:   gateway.CreateTicket != nil,
			TicketUpdate:   gateway.UpdateTicket != nil,
			TicketActions:  []string{},
			MaxPageSize:    repositoryTicketMaxPageSize,
			MaxSearchBytes: repositoryTicketMaxSearchBytes,
		},
	}
	if gateway.GetTicket != nil {
		capabilities.Repositories.TicketBodyBudgetBytes = gateway.TicketBodyBudgetBytes
	}
	if gateway.MutateTicket != nil {
		capabilities.Repositories.TicketActions = repositoryMutationOperations()
	}
	control := s.config.Control
	if control == nil {
		return capabilities
	}
	if control.PauseDaemon != nil {
		capabilities.DaemonActions = append(capabilities.DaemonActions, "pause")
	}
	if control.ResumeDaemon != nil {
		capabilities.DaemonActions = append(capabilities.DaemonActions, "resume")
	}
	if control.AbortDaemon != nil {
		capabilities.DaemonActions = append(capabilities.DaemonActions, "abort")
	}
	if control.Reload != nil {
		capabilities.DaemonActions = append(capabilities.DaemonActions, "reload")
	}
	if control.Doctor != nil {
		capabilities.DaemonActions = append(capabilities.DaemonActions, "doctor")
	}
	if control.Shutdown != nil {
		capabilities.DaemonActions = append(capabilities.DaemonActions, "shutdown")
	}
	if control.StartWorker != nil {
		capabilities.WorkerActions = append(capabilities.WorkerActions, "start")
	}
	if control.StopWorker != nil {
		capabilities.WorkerActions = append(capabilities.WorkerActions, "stop")
	}
	if control.PauseWorker != nil {
		capabilities.WorkerActions = append(capabilities.WorkerActions, "pause")
	}
	if control.ResumeWorker != nil {
		capabilities.WorkerActions = append(capabilities.WorkerActions, "resume")
	}
	if control.RestartWorker != nil {
		capabilities.WorkerActions = append(capabilities.WorkerActions, "restart")
	}
	if control.StartGroup != nil {
		capabilities.GroupActions = append(capabilities.GroupActions, "start")
	}
	if control.StopGroup != nil {
		capabilities.GroupActions = append(capabilities.GroupActions, "stop")
	}
	return capabilities
}
