package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/toolsupply/ticket-orc/internal/supervisor"
)

const capabilitiesTestInstanceID = "1e4f5f6d-3a59-49f6-8c2f-e18186ac45aa"
const capabilitiesTestPath = "/private/repository/path"

func statusForCapabilities(t *testing.T, control *Control, gateway RepositoryGateway, workers []WorkerStatus) (Status, []byte) {
	t.Helper()
	server, err := NewServer(Config{
		StateDir:    t.TempDir(),
		InstanceID:  capabilitiesTestInstanceID,
		EndpointKey: testEndpointKey,
		Version:     "runtime-version",
		Status: func() Status {
			return Status{
				Workers:      workers,
				Repositories: []RepositoryStatus{{ID: testRepositoryID, Key: "project", Path: capabilitiesTestPath, State: "healthy"}},
			}
		},
		Control:           control,
		RepositoryGateway: gateway,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.handleStatus(response, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status response=%d body=%q", response.Code, response.Body.String())
	}
	var status Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	return status, response.Body.Bytes()
}

func fullCapabilityControl() *Control {
	return &Control{
		PauseDaemon: func(context.Context) (DaemonControlResult, error) { return DaemonControlResult{}, nil },
		ResumeDaemon: func(context.Context) (DaemonControlResult, error) {
			return DaemonControlResult{}, nil
		},
		AbortDaemon: func(context.Context) (DaemonControlResult, error) { return DaemonControlResult{}, nil },
		StartWorker: func(context.Context, string) (supervisor.MutationResult, error) {
			return supervisor.MutationResult{}, nil
		},
		StopWorker: func(context.Context, string) (supervisor.MutationResult, error) {
			return supervisor.MutationResult{}, nil
		},
		PauseWorker: func(context.Context, string) (supervisor.MutationResult, error) {
			return supervisor.MutationResult{}, nil
		},
		ResumeWorker: func(context.Context, string) (supervisor.MutationResult, error) {
			return supervisor.MutationResult{}, nil
		},
		RestartWorker: func(context.Context, string) (supervisor.MutationResult, error) {
			return supervisor.MutationResult{}, nil
		},
		StartGroup: func(context.Context, string) (supervisor.GroupResult, error) {
			return supervisor.GroupResult{}, nil
		},
		StopGroup: func(context.Context, string) (supervisor.GroupResult, error) {
			return supervisor.GroupResult{}, nil
		},
		Reload: func(context.Context) (supervisor.ReloadResult, error) {
			return supervisor.ReloadResult{}, nil
		},
		Doctor: func(context.Context) (supervisor.DoctorResult, error) {
			return supervisor.DoctorResult{}, nil
		},
		Shutdown: func(context.Context) error { return nil },
	}
}

func fullCapabilityGateway() RepositoryGateway {
	return RepositoryGateway{
		ListTickets: func(context.Context, string, RepositoryTicketQuery) (RepositoryTicketList, error) {
			return RepositoryTicketList{}, nil
		},
		GetTicket: func(context.Context, string, string) (RepositoryTicketDetail, error) {
			return RepositoryTicketDetail{}, nil
		},
		CreateTicket: func(context.Context, string, RepositoryTicketCreateRequest) (RepositoryTicketMutation, error) {
			return RepositoryTicketMutation{}, nil
		},
		UpdateTicket: func(context.Context, string, string, RepositoryTicketUpdateRequest) (RepositoryTicketMutation, error) {
			return RepositoryTicketMutation{}, nil
		},
		MutateTicket: func(context.Context, string, string, string, RepositoryTicketMutationRequest) (RepositoryTicketMutation, error) {
			return RepositoryTicketMutation{}, nil
		},
		TicketBodyBudgetBytes: 256 << 10,
	}
}

func TestStatusAdvertisesFullDaemonCapabilities(t *testing.T) {
	status, raw := statusForCapabilities(t, fullCapabilityControl(), fullCapabilityGateway(), []WorkerStatus{{Name: "coder", State: "running"}})
	want := `{"events":true,"daemon_actions":["pause","resume","abort","reload","doctor","shutdown"],"worker_actions":["start","stop","pause","resume","restart"],"group_actions":["start","stop"],"repositories":{"inventory":true,"ticket_list":true,"ticket_detail":true,"ticket_create":true,"ticket_update":true,"ticket_actions":["claim","release","open","hold","submit","review","approve","close","reject","bump"],"max_page_size":256,"max_search_bytes":256,"ticket_body_budget_bytes":262144}}`
	capabilityJSON, err := json.Marshal(status.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if string(capabilityJSON) != want {
		t.Fatalf("capabilities JSON = %s, want %s", capabilityJSON, want)
	}
	if !strings.Contains(string(raw), `"protocol":1`) || !strings.Contains(string(raw), `"capabilities":`+want) {
		t.Fatalf("status JSON = %s, want protocol 1 and the exact capability object", raw)
	}
	if strings.Contains(string(raw), testEndpointKey) || strings.Contains(string(capabilityJSON), capabilitiesTestPath) {
		t.Fatalf("capabilities exposed a secret or repository path: status=%s capabilities=%s", raw, capabilityJSON)
	}
}

func TestStatusAdvertisesRepositoryOnlyCapabilitiesWithoutWorkers(t *testing.T) {
	status, raw := statusForCapabilities(t, nil, fullCapabilityGateway(), []WorkerStatus{})
	capabilities := status.Capabilities
	if !capabilities.Events || len(capabilities.DaemonActions) != 0 || len(capabilities.WorkerActions) != 0 || len(capabilities.GroupActions) != 0 {
		t.Fatalf("zero-worker capabilities = %#v", capabilities)
	}
	if !capabilities.Repositories.Inventory || !capabilities.Repositories.TicketList || !capabilities.Repositories.TicketDetail || !capabilities.Repositories.TicketCreate || !capabilities.Repositories.TicketUpdate || len(capabilities.Repositories.TicketActions) != 10 {
		t.Fatalf("repository-only capabilities = %#v", capabilities.Repositories)
	}
	want := `"capabilities":{"events":true,"daemon_actions":[],"worker_actions":[],"group_actions":[],"repositories":{"inventory":true,"ticket_list":true,"ticket_detail":true,"ticket_create":true,"ticket_update":true,"ticket_actions":["claim","release","open","hold","submit","review","approve","close","reject","bump"],"max_page_size":256,"max_search_bytes":256,"ticket_body_budget_bytes":262144}}`
	if !strings.Contains(string(raw), want) {
		t.Fatalf("repository-only status JSON = %s, want literal capabilities %s", raw, want)
	}
}

func TestStatusOmitsCapabilitiesForMissingCallbacks(t *testing.T) {
	controlCases := []struct {
		name   string
		remove func(*Control)
		group  string
		want   []string
	}{
		{name: "daemon pause", remove: func(c *Control) { c.PauseDaemon = nil }, group: "daemon", want: []string{"resume", "abort", "reload", "doctor", "shutdown"}},
		{name: "daemon resume", remove: func(c *Control) { c.ResumeDaemon = nil }, group: "daemon", want: []string{"pause", "abort", "reload", "doctor", "shutdown"}},
		{name: "daemon abort", remove: func(c *Control) { c.AbortDaemon = nil }, group: "daemon", want: []string{"pause", "resume", "reload", "doctor", "shutdown"}},
		{name: "reload", remove: func(c *Control) { c.Reload = nil }, group: "daemon", want: []string{"pause", "resume", "abort", "doctor", "shutdown"}},
		{name: "doctor", remove: func(c *Control) { c.Doctor = nil }, group: "daemon", want: []string{"pause", "resume", "abort", "reload", "shutdown"}},
		{name: "shutdown", remove: func(c *Control) { c.Shutdown = nil }, group: "daemon", want: []string{"pause", "resume", "abort", "reload", "doctor"}},
		{name: "worker start", remove: func(c *Control) { c.StartWorker = nil }, group: "worker", want: []string{"stop", "pause", "resume", "restart"}},
		{name: "worker stop", remove: func(c *Control) { c.StopWorker = nil }, group: "worker", want: []string{"start", "pause", "resume", "restart"}},
		{name: "worker pause", remove: func(c *Control) { c.PauseWorker = nil }, group: "worker", want: []string{"start", "stop", "resume", "restart"}},
		{name: "worker resume", remove: func(c *Control) { c.ResumeWorker = nil }, group: "worker", want: []string{"start", "stop", "pause", "restart"}},
		{name: "worker restart", remove: func(c *Control) { c.RestartWorker = nil }, group: "worker", want: []string{"start", "stop", "pause", "resume"}},
		{name: "group start", remove: func(c *Control) { c.StartGroup = nil }, group: "group", want: []string{"stop"}},
		{name: "group stop", remove: func(c *Control) { c.StopGroup = nil }, group: "group", want: []string{"start"}},
	}
	for _, test := range controlCases {
		t.Run(test.name, func(t *testing.T) {
			control := fullCapabilityControl()
			test.remove(control)
			status, raw := statusForCapabilities(t, control, fullCapabilityGateway(), nil)
			var got []string
			switch test.group {
			case "daemon":
				got = status.Capabilities.DaemonActions
			case "worker":
				got = status.Capabilities.WorkerActions
			case "group":
				got = status.Capabilities.GroupActions
			}
			if !equalStrings(got, test.want) {
				t.Fatalf("%s actions = %#v, want %#v", test.group, got, test.want)
			}
			encoded, err := json.Marshal(test.want)
			if err != nil {
				t.Fatal(err)
			}
			field := `"` + test.group + `_actions":` + string(encoded)
			if !strings.Contains(string(raw), field) {
				t.Fatalf("status JSON = %s, want literal field %s", raw, field)
			}
		})
	}

	repositoryCases := []struct {
		name   string
		remove func(*RepositoryGateway)
		check  func(RepositoryCapabilities) bool
	}{
		{name: "list", remove: func(g *RepositoryGateway) { g.ListTickets = nil }, check: func(c RepositoryCapabilities) bool { return !c.TicketList }},
		{name: "detail", remove: func(g *RepositoryGateway) { g.GetTicket = nil }, check: func(c RepositoryCapabilities) bool { return !c.TicketDetail && c.TicketBodyBudgetBytes == 0 }},
		{name: "create", remove: func(g *RepositoryGateway) { g.CreateTicket = nil }, check: func(c RepositoryCapabilities) bool { return !c.TicketCreate }},
		{name: "update", remove: func(g *RepositoryGateway) { g.UpdateTicket = nil }, check: func(c RepositoryCapabilities) bool { return !c.TicketUpdate }},
		{name: "ticket actions", remove: func(g *RepositoryGateway) { g.MutateTicket = nil }, check: func(c RepositoryCapabilities) bool { return len(c.TicketActions) == 0 }},
	}
	for _, test := range repositoryCases {
		t.Run("repository "+test.name, func(t *testing.T) {
			gateway := fullCapabilityGateway()
			test.remove(&gateway)
			status, raw := statusForCapabilities(t, nil, gateway, nil)
			if !test.check(status.Capabilities.Repositories) {
				t.Fatalf("repository capabilities = %#v", status.Capabilities.Repositories)
			}
			if test.name == "detail" && !strings.Contains(string(raw), `"ticket_detail":false`) {
				t.Fatalf("status JSON = %s, want ticket_detail false", raw)
			}
		})
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
