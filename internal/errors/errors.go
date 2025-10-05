package errors

import (
	"errors"
	"fmt"
	"strings"
)

type FixedCause struct {
	Message string
	Err     error
}

func (e *FixedCause) Error() string {
	return e.Message + ": " + e.Err.Error()
}

func (e *FixedCause) Unwrap() error {
	return e.Err
}

func NewFixedCause(message string, err error) error {
	return &FixedCause{Message: message, Err: err}
}

type ErrorAggregation struct {
	Errors      []error
	FixedCauses map[string]int64
	MaxErrors   int
}

func NewErrorAggregation(maxErrors int) *ErrorAggregation {
	return &ErrorAggregation{
		MaxErrors: maxErrors,
	}
}

func (e *ErrorAggregation) Error() string {
	var sb strings.Builder
	if e.FixedCauses != nil {
		sb.WriteString(fmt.Sprintf("aggregated errors with %d distinct causes:\n", len(e.FixedCauses)))
		for cause, count := range e.FixedCauses {
			sb.WriteString(fmt.Sprintf(" - %s: %d\n", cause, count))
		}
	}
	if len(e.Errors) > 0 {
		sb.WriteString("individual errors:\n")
		for _, err := range e.Errors {
			sb.WriteString(fmt.Sprintf(" - %s\n", err.Error()))
		}
	}
	return sb.String()
}

func (e *ErrorAggregation) Add(err error) {
	var fixedCause *FixedCause
	if errors.As(err, &fixedCause) {
		if e.FixedCauses == nil {
			e.FixedCauses = make(map[string]int64)
		}
		e.FixedCauses[fixedCause.Message] += 1
	} else if e.MaxErrors <= 0 || len(e.Errors) < e.MaxErrors {
		e.Errors = append(e.Errors, err)
	}
}
