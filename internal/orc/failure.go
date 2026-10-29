package orc

import "github.com/toolsupply/ticket-orc/internal/harness"

// FailureContext carries bounded operational metadata independently of an
// error's human-readable message.
type FailureContext struct {
	// Execution may describe a completed harness followed by an Orc failure.
	// It is evidence, never an assertion that the harness itself failed.
	Execution    *harness.ExecutionEvidence
	Origin       string
	Operation    string
	Phase        string
	Ticket       string
	Contained    bool
	RepositoryID string
	Worker       string
	Role         string
	Actor        string
	AttemptID    uint64
}

// FailureContextError preserves a cause while providing structured context
// for lifecycle and supervisor classification.
type FailureContextError struct {
	Context FailureContext
	Cause   error
}

func (e *FailureContextError) Error() string {
	if e == nil || e.Cause == nil {
		return "worker operation failed"
	}
	return e.Cause.Error()
}

func (e *FailureContextError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewFailureContextError(metadata FailureContext, cause error) error {
	if cause == nil {
		return nil
	}
	return &FailureContextError{Context: metadata, Cause: cause}
}
