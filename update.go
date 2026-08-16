package memcache

import (
	"context"
	"fmt"
	"time"
)

// updateAttempts bounds the optimistic concurrency loops in Update and Take.
const updateAttempts = 8

// Update atomically transforms a value and stores the result for ttl: read
// with version, apply fn, write back only if unchanged, retry on conflict.
// Version tokens never appear in user code. On a miss fn receives (nil,
// false); returning an error from fn aborts the whole operation without
// writing. fn may run multiple times and must be pure. A value kept stale by
// Invalidate is treated as a miss: fn transforms rather than recomputes, and
// transforming invalidated data would silently launder it back to fresh.
func (c *Client) Update(ctx context.Context, key string, ttl time.Duration, fn func(current []byte, found bool) ([]byte, error)) ([]byte, error) {
	if fn == nil {
		return nil, fmt.Errorf("memcache: Update requires a transform function")
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return nil, err
	}
	readOptions := baseReadOptions()
	for range updateAttempts {
		command, err := buildGet(key, readOptions)
		if err != nil {
			return nil, err
		}
		wire, err := c.executeMeta(ctx, command)
		if err != nil {
			return nil, err
		}
		result, err := semanticGet(key, readOptions, wire)
		if err != nil {
			return nil, err
		}
		staleWin := result.ValueState == ValueStale && result.Lease == LeaseGranted
		var current []byte
		found := false
		var compareCAS *uint64
		if result.Status == GetHit {
			// A stale or zero-byte hit reads as a miss, but the entry still
			// exists: only a compare-and-swap write can replace it.
			compareCAS = result.Metadata.CAS
			if result.ValueState == ValueFresh && len(result.Value) > 0 {
				current, found = result.Value, true
			}
		}
		next, err := fn(current, found)
		if err != nil {
			// Aborting leaves the entry unwritten; an accidentally consumed
			// stale-recache token must go back so Fetch can still elect.
			if staleWin {
				c.returnStaleWin(key, result)
			}
			return nil, err
		}
		if len(next) == 0 {
			if staleWin {
				c.returnStaleWin(key, result)
			}
			return nil, errEmptyValue
		}
		writeOptions := MetaSetOptions{TTL: expiration}
		if compareCAS != nil {
			writeOptions.CompareCAS = compareCAS
		} else {
			writeOptions.Mode = ModeAdd
		}
		status, err := c.doSet(ctx, key, next, writeOptions)
		if err != nil {
			return nil, err
		}
		if status == MutationApplied {
			return next, nil
		}
	}
	return nil, ErrConflict
}

// Take atomically reads a value and deletes it: read with version, delete
// only if unchanged, retry when a concurrent write slipped in between. Bytes
// appended between the read and the delete are never lost. A nil result
// means there was nothing to take; a miss and an empty value are
// deliberately the same answer. The result feeds the caller's next step, so
// Degrade never absorbs failures here.
func (c *Client) Take(ctx context.Context, key string) ([]byte, error) {
	readOptions := baseReadOptions()
	for range updateAttempts {
		command, err := buildGet(key, readOptions)
		if err != nil {
			return nil, err
		}
		wire, err := c.executeMeta(ctx, command)
		if err != nil {
			return nil, err
		}
		result, err := semanticGet(key, readOptions, wire)
		if err != nil {
			return nil, err
		}
		if result.Status != GetHit || len(result.Value) == 0 {
			if result.ValueState == ValueStale && result.Lease == LeaseGranted {
				c.returnStaleWin(key, result)
			}
			return nil, nil
		}
		if result.Metadata.CAS == nil {
			return nil, &ProtocolError{Message: "value read omitted requested CAS"}
		}
		command, err = buildDelete(key, MetaDeleteOptions{CompareCAS: result.Metadata.CAS})
		if err != nil {
			return nil, err
		}
		wire, err = c.executeMeta(ctx, command)
		if err != nil {
			return nil, err
		}
		status, err := deleteStatus(wire.Code)
		if err != nil {
			return nil, err
		}
		if status == MutationApplied {
			return result.Value, nil
		}
		// A CAS mismatch means the value changed after the read; a vanished
		// key means someone else deleted it. Either way, read again.
	}
	return nil, ErrConflict
}
