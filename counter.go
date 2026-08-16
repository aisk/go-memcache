package memcache

import (
	"context"
	"fmt"
	"time"
)

// counter is the shared implementation of Incr and Decr.
func (c *Client) counter(ctx context.Context, key string, delta uint64, decrement bool, ttl time.Duration) (uint64, error) {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return 0, err
	}
	// A miss counts from zero: seeding the vivified item with the delta (or
	// zero when decrementing toward the floor) makes the first operation
	// behave as zero plus delta.
	initial := delta
	if decrement {
		initial = 0
	}
	arithmetic := MetaArithmeticOptions{Delta: delta, Decrement: decrement, Initial: &initial, InitialTTL: &expiration}
	command, err := buildArithmetic(key, arithmetic)
	if err != nil {
		return 0, err
	}
	wire, err := c.executeMeta(ctx, command)
	if err != nil {
		return 0, err
	}
	result, err := semanticArithmetic(key, arithmetic, wire)
	if err != nil {
		return 0, err
	}
	if !result.HasValue {
		return 0, fmt.Errorf("memcache: counter %q was not updated", key)
	}
	return result.Value, nil
}

// Incr adds delta to a decimal counter, creating it with ttl on a miss so
// the first request counts as delta. The ttl applies only at creation; later
// increments never extend an existing counter's lifetime, which is exactly
// what fixed-window counting needs. The result feeds business decisions, so
// Degrade never fakes one: infrastructure failures surface.
func (c *Client) Incr(ctx context.Context, key string, delta uint64, ttl time.Duration) (uint64, error) {
	return c.counter(ctx, key, delta, false, ttl)
}

// Decr subtracts delta from a decimal counter, saturating at zero. A miss
// creates the counter at zero with ttl; as with Incr, the ttl applies only
// at creation.
func (c *Client) Decr(ctx context.Context, key string, delta uint64, ttl time.Duration) (uint64, error) {
	return c.counter(ctx, key, delta, true, ttl)
}
