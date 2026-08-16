package memcache

import (
	"context"
	"fmt"
	"time"
)

// Delete removes a key. Deleting an absent key is a success: the goal state
// already holds.
func (c *Client) Delete(ctx context.Context, key string) error {
	command, err := buildDelete(key, MetaDeleteOptions{})
	if err != nil {
		return err
	}
	wire, err := c.executeMeta(ctx, command)
	if err != nil {
		if c.absorb(ctx, err) {
			return nil
		}
		return err
	}
	status, err := deleteStatus(wire.Code)
	if err != nil {
		return err
	}
	if status != MutationApplied && status != MutationNotFound {
		return fmt.Errorf("memcache: delete of %q was not applied", key)
	}
	return nil
}

// DeleteMany removes a set of keys in one round trip per backend.
func (c *Client) DeleteMany(ctx context.Context, keys []string) error {
	operations := make([]Operation, len(keys))
	for i, key := range keys {
		operations[i] = DeleteOperation{Key: key}
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

// Invalidate marks a value stale instead of dropping it. For the grace
// period, readers keep the old copy while Fetch elects one caller to
// recompute in the background; afterwards the key decays into a normal miss.
// The grace bound also guarantees recovery when an elected recomputer crashes
// without writing: the entry dies on schedule and the next Fetch re-elects.
// It is an upper bound only while nothing renews the key, since Touch slides
// it like any other expiration. Invalidate pairs with Fetch-managed keys; use
// Delete when the old value must not be served for even a second.
func (c *Client) Invalidate(ctx context.Context, key string, grace time.Duration) error {
	if grace <= 0 {
		return fmt.Errorf("memcache: Invalidate grace must be positive")
	}
	staleFor := ExpiresIn(grace)
	command, err := buildDelete(key, MetaDeleteOptions{Invalidate: true, StaleFor: &staleFor})
	if err != nil {
		return err
	}
	wire, err := c.executeMeta(ctx, command)
	if err != nil {
		if c.absorb(ctx, err) {
			return nil
		}
		return err
	}
	// A missing key is already as invalid as it gets.
	_, err = deleteStatus(wire.Code)
	return err
}
