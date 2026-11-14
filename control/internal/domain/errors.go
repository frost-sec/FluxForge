package domain

import "fmt"

type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }
func E(code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

const (
	Invalid     = "invalid"
	NotFound    = "not_found"
	Conflict    = "conflict"
	Stale       = "stale"
	Unavailable = "unavailable"
	Exhausted   = "exhausted"
)
