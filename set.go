package memcache

import (
	"context"
	"fmt"
	"time"
)

// doSet executes one write and reports its outcome.
func (c *Client) doSet(ctx context.Context, key string, value []byte, options MetaSetOptions) (MutationStatus, error) {
	command, err := buildSet(key, value, options)
	if err != nil {
		return MutationUnknown, err
	}
	wire, err := c.executeMeta(ctx, command)
	if err != nil {
		return MutationUnknown, err
	}
	return storeStatus(wire.Code, options.Mode)
}

// Set unconditionally stores a value for ttl. Storing without expiration is
// the explicit choice Forever. The value is encoded with the client's codec;
// a []byte value is stored as is.
func (c *Client) Set[T any](ctx context.Context, key string, value T, ttl time.Duration) error {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return err
	}
	data, err := c.encode(value)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errEmptyValue
	}
	status, err := c.doSet(ctx, key, data, MetaSetOptions{TTL: expiration})
	if err != nil {
		if c.absorb(ctx, err) {
			return nil
		}
		return err
	}
	if status != MutationApplied {
		return fmt.Errorf("memcache: set of %q was not stored", key)
	}
	return nil
}

// SetMany stores a set of values in one round trip per backend, all sharing
// the same ttl.
func (c *Client) SetMany[T any](ctx context.Context, mapping map[string]T, ttl time.Duration) error {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return err
	}
	operations := make([]Operation, 0, len(mapping))
	for key, value := range mapping {
		data, err := c.encode(value)
		if err != nil {
			return fmt.Errorf("memcache: value for %q: %w", key, err)
		}
		if len(data) == 0 {
			return fmt.Errorf("memcache: value for %q: %w", key, errEmptyValue)
		}
		operations = append(operations, SetOperation{Key: key, Value: data, Options: MetaSetOptions{TTL: expiration}})
	}
	results, err := c.batch(ctx, operations)
	if err != nil {
		return err
	}
	var firstErr error
	for _, result := range results {
		if result.Err != nil && !c.absorb(ctx, result.Err) && firstErr == nil {
			firstErr = result.Err
		}
	}
	return firstErr
}

// Add stores only when the key is absent and reports whether this caller won.
// The bool is the caller's whole answer, so Add keeps it; it is also why
// Degrade never fakes a result here.
func (c *Client) Add[T any](ctx context.Context, key string, value T, ttl time.Duration) (bool, error) {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.encode(value)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, errEmptyValue
	}
	status, err := c.doSet(ctx, key, data, MetaSetOptions{TTL: expiration, Mode: ModeAdd})
	if err != nil {
		return false, err
	}
	return status == MutationApplied, nil
}

// Replace stores only when the key still exists and reports whether it did.
// It is the write half of session renewal: false means the session ended
// mid-request and there is nothing to write back to.
func (c *Client) Replace[T any](ctx context.Context, key string, value T, ttl time.Duration) (bool, error) {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.encode(value)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, errEmptyValue
	}
	status, err := c.doSet(ctx, key, data, MetaSetOptions{TTL: expiration, Mode: ModeReplace})
	if err != nil {
		return false, err
	}
	return status == MutationApplied, nil
}

// concat is the shared implementation of Append and Prepend.
func (c *Client) concat(ctx context.Context, key string, fragment []byte, mode StoreMode, ttl time.Duration) error {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return err
	}
	if len(fragment) == 0 {
		return errEmptyValue
	}
	status, err := c.doSet(ctx, key, fragment, MetaSetOptions{Mode: mode, VivifyTTL: &expiration})
	if err != nil {
		if c.absorb(ctx, err) {
			return nil
		}
		return err
	}
	if status != MutationApplied {
		return ErrNotStored
	}
	return nil
}

// Append adds a fragment to the end of a raw bytes value, creating the value
// with ttl on a miss. The ttl applies only at creation; later appends never
// extend an existing value's lifetime. Fragments bypass any value encoding;
// how the accumulated bytes are structured is the caller's business.
func (c *Client) Append(ctx context.Context, key string, fragment []byte, ttl time.Duration) error {
	return c.concat(ctx, key, fragment, ModeAppend, ttl)
}

// Prepend adds a fragment to the front of a raw bytes value, creating the
// value with ttl on a miss; as with Append, the ttl applies only at creation.
func (c *Client) Prepend(ctx context.Context, key string, fragment []byte, ttl time.Duration) error {
	return c.concat(ctx, key, fragment, ModePrepend, ttl)
}
