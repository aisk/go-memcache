package memcache

import (
	"errors"
	"fmt"
)

var (
	// ErrCacheMiss is returned by Get when the key is absent.
	ErrCacheMiss = errors.New("memcache: cache miss")
	// ErrClosed is returned after a client has been closed.
	ErrClosed = errors.New("memcache: client is closed")
	// ErrNotStored means a conditional mutation was not applied.
	ErrNotStored = errors.New("memcache: value not stored")
	// ErrCASMismatch means the supplied CAS token no longer matches.
	ErrCASMismatch = errors.New("memcache: CAS mismatch")
)

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
type AmbiguousWriteError struct {
	Operation string
	Key       string
	Cause     error
}

func (e *AmbiguousWriteError) Error() string {
	return fmt.Sprintf("memcache: outcome of %s for %q is ambiguous: %v", e.Operation, e.Key, e.Cause)
}

func (e *AmbiguousWriteError) Unwrap() error { return e.Cause }
