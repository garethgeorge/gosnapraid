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
	Errors               []error
	FixedCausesExemplars []error
	FixedCauses          map[string]int64
	MaxErrors            int
}

func NewErrorAggregation(maxErrors int) *ErrorAggregation {
	return &ErrorAggregation{
		MaxErrors: maxErrors,
	}
}

func (e *ErrorAggregation) HasErrors() bool {
	return len(e.Errors) > 0 || len(e.FixedCauses) > 0
}

func (e *ErrorAggregation) Error() string {
	var sb strings.Builder
	if e.FixedCauses != nil {
		sb.WriteString(fmt.Sprintf("aggregated errors with %d distinct causes:\n", len(e.FixedCauses)))
		for cause, count := range e.FixedCauses {
			sb.WriteString(fmt.Sprintf(" - %s: %d\n", cause, count))
		}
		sb.WriteString("exemplars for each cause:\n")
		for _, err := range e.FixedCausesExemplars {
			sb.WriteString(fmt.Sprintf(" - %s\n", err.Error()))
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
		if _, ok := e.FixedCauses[fixedCause.Message]; !ok {
			e.FixedCausesExemplars = append(e.FixedCausesExemplars, fixedCause)
		}
		e.FixedCauses[fixedCause.Message] += 1
	} else if e.MaxErrors <= 0 || len(e.Errors) < e.MaxErrors {
		e.Errors = append(e.Errors, err)
	}
}
