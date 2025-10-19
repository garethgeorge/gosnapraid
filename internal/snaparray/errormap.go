package snaparray

import "strings"

type ErrorMap struct {
	Title  string
	Errors map[string]error
}

func (e ErrorMap) Error() string {
	if len(e.Errors) == 0 {
		return ""
	}

	// create a string representing all errors
	builder := strings.Builder{}
	if e.Title != "" {
		builder.WriteString(e.Title + ":\n")
	} else {
		builder.WriteString("Errors:\n")
	}
	for path, err := range e.Errors {
		builder.WriteString(path)
		builder.WriteString(": ")
		builder.WriteString(err.Error())
		builder.WriteString("\n")
	}
	return builder.String()
}

func (e ErrorMap) AddError(path string, err error) {
	if e.Errors == nil {
		e.Errors = make(map[string]error)
	}
	e.Errors[path] = err
}

func (e ErrorMap) HasErrors() bool {
	return len(e.Errors) > 0
}
