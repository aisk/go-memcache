package memcache

import (
	"errors"
	"fmt"
)

var (
	// ErrCacheMiss is a protocol-layer sentinel returned by MetaClient.Debug
	// when the key is absent. Scenario verbs never return it: a miss is a
	// normal answer there, expressed by the ok result or key absence.
	ErrCacheMiss = errors.New("memcache: cache miss")
	// ErrClosed is returned after a client has been closed.
	ErrClosed = errors.New("memcache: client is closed")
	// ErrNotStored means a conditional mutation was not applied.
	ErrNotStored = errors.New("memcache: value not stored")
	// ErrConflict is returned by Update and Take when their optimistic
	// retry loop keeps losing to concurrent writers.
	ErrConflict = errors.New("memcache: too many conflicting concurrent writes")
)

// usageError marks a client-side mistake (invalid key, oversized value,
// malformed command) detected before any network exchange. It is permanent:
// no retry can fix it, so Degrade never absorbs it into a fake result.
type usageError struct{ err error }

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

// ProtocolError reports malformed or unexpected data from a server.
type ProtocolError struct{ Message string }

func (e *ProtocolError) Error() string { return "memcache: protocol error: " + e.Message }

// ServerError represents an ERROR, CLIENT_ERROR, or SERVER_ERROR response.
type ServerError struct {
	Kind    string
	Message string
}

func (e *ServerError) Error() string {
	if e.Message == "" {
		return "memcache: " + e.Kind
	}
	return fmt.Sprintf("memcache: %s: %s", e.Kind, e.Message)
}

// AmbiguousWriteError means a side-effecting request reached the connection,
// but its result could not be observed. Retrying it may duplicate the effect.
// It is the one error class Degrade never absorbs: degrading covers "the
// cache is unavailable", not "the write may or may not have landed".
type AmbiguousWriteError struct {
	Operation string
	Key       string
	Cause     error
}

func (e *AmbiguousWriteError) Error() string {
	return fmt.Sprintf("memcache: outcome of %s for %q is ambiguous: %v", e.Operation, e.Key, e.Cause)
}

func (e *AmbiguousWriteError) Unwrap() error { return e.Cause }
