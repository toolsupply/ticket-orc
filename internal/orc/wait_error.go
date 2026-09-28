package orc

import "fmt"

// WorkWaitError marks a failure while waiting for the next role ticket. The
// role is explicit so command output does not have to infer it from wording.
type WorkWaitError struct {
	Role  string
	Cause error
}

func (e *WorkWaitError) Error() string {
	if e == nil {
		return "wait for Ticket work failed"
	}
	label := "review"
	if e.Role == "coder" {
		label = "implementation"
	}
	if e.Cause == nil {
		return fmt.Sprintf("wait for %s work failed", label)
	}
	return fmt.Sprintf("wait for %s work: %v", label, e.Cause)
}

func (e *WorkWaitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
