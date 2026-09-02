package memcache

import (
	"context"
	"fmt"
	"time"
)

// doGet executes one single-key read. Usage errors surface as-is;
// infrastructure failures are folded into a miss when Degrade is enabled.
// An accidental stale-recache win is handed back to the server.
func (c *Client) doGet(ctx context.Context, key string, options MetaGetOptions) (GetResult, error) {
	command, err := buildGet(key, options)
	if err != nil {
		return GetResult{}, err
	}
	wire, err := c.executeMeta(ctx, command)
	if err == nil {
		var result GetResult
		if result, err = semanticGet(key, options, wire); err == nil {
			if result.ValueState == ValueStale && result.Lease == LeaseGranted {
				c.returnStaleWin(key, result)
			}
			return result, nil
		}
	}
	if c.absorb(ctx, err) {
		return GetResult{Key: key, Status: GetMiss, ValueState: ValueMissing}, nil
	}
	return GetResult{}, err
}

// baseReadOptions is the default flag set for reads. CAS and TTL are
// requested so an accidental stale-recache win can be handed back.
func baseReadOptions() MetaGetOptions {
	return MetaGetOptions{ReturnCAS: true, ReturnTTL: true}
}

// policyReadOptions resolves per-call read options into the wire flag set.
func policyReadOptions(policy callPolicy) (MetaGetOptions, error) {
	read := baseReadOptions()
	if policy.touch != nil {
		expiration, err := resolveTTL(*policy.touch)
		if err != nil {
			return MetaGetOptions{}, err
		}
		read.Touch = &expiration
	}
	return read, nil
}

// returnStaleWin hands an accidental stale-recache win back to the server.
// memcached grants a stale entry's single recache token to the first reader
// regardless of what it asked for, and never re-grants it until the entry is
// written. A read without a loader cannot recompute anything, so keeping the
// token would silently disable the Fetch election for the rest of the grace
// period. Re-invalidating with the CAS just read resets the token without
// changing the value or shrinking the window.
func (c *Client) returnStaleWin(key string, result GetResult) {
	cas, ttl := result.Metadata.CAS, result.Metadata.TTL
	if cas == nil || ttl == nil {
		return
	}
	options := MetaDeleteOptions{Invalidate: true, CompareCAS: cas}
	if *ttl > 0 {
		// The remaining TTL is raw seconds; beyond 30 days memcached would
		// read it as a Unix timestamp, so it must go through ExpiresIn.
		staleFor := ExpiresIn(time.Duration(*ttl) * time.Second)
		options.StaleFor = &staleFor
	}
	go func() {
		command, err := buildDelete(key, options)
		if err == nil {
			// A compare mismatch means the entry moved on and the token no
			// longer matters; only transport failures are worth reporting.
			_, err = c.executeMeta(c.rootCtx, command)
		}
		if err != nil {
			c.reportError(fmt.Errorf("memcache: returning the stale refresh election for %q: %w", key, err))
		}
	}()
}

// Get reads a value. A miss is a normal answer, not an error: ok reports
// presence, err reports infrastructure failure, and the two never mix. A
// value kept stale by Invalidate is returned as an ordinary hit. The Touch
// option makes the same command also slide the hit's expiration. T is
// decoded with the client's codec; Get[[]byte] returns the raw bytes.
func (c *Client) Get[T any](ctx context.Context, key string, options ...GetOption) (T, bool, error) {
	var zero T
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return zero, false, errNilOption
		}
		option.applyGet(&policy)
	}
	read, err := policyReadOptions(policy)
	if err != nil {
		return zero, false, err
	}
	result, err := c.doGet(ctx, key, read)
	if err != nil {
		return zero, false, err
	}
	if result.Status != GetHit || len(result.Value) == 0 {
		return zero, false, nil
	}
	value, err := decode[T](c, result.Value)
	if err != nil {
		return zero, false, err
	}
	return value, true, nil
}

// GetMany reads a set of keys in one round trip per backend and returns the
// hits; a miss is expressed by key absence. With Degrade enabled a failing
// backend only removes its own keys from the result. Values are decoded like
// Get; a value that fails to decode is left out and reported as the error.
func (c *Client) GetMany[T any](ctx context.Context, keys []string, options ...GetOption) (map[string]T, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return nil, errNilOption
		}
		option.applyGet(&policy)
	}
	read, err := policyReadOptions(policy)
	if err != nil {
		return nil, err
	}
	operations := make([]Operation, len(keys))
	for i, key := range keys {
		operations[i] = GetOperation{Key: key, Options: read}
	}
	results, err := c.batch(ctx, operations)
	if err != nil {
		return nil, err
	}
	found := make(map[string]T, len(keys))
	var firstErr error
	for i, result := range results {
		if result.Err != nil {
			if !c.absorb(ctx, result.Err) && firstErr == nil {
				firstErr = result.Err
			}
			continue
		}
		if result.Get == nil || !result.Get.Hit() {
			continue
		}
		if result.Get.ValueState == ValueStale && result.Get.Lease == LeaseGranted {
			c.returnStaleWin(keys[i], *result.Get)
		}
		if len(result.Get.Value) == 0 {
			continue
		}
		value, err := decode[T](c, result.Get.Value)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("memcache: %q: %w", keys[i], err)
			}
			continue
		}
		found[keys[i]] = value
	}
	return found, firstErr
}

// Touch extends a key's TTL without transferring its value, as one blind
// protocol command. A missing key is not an error; there is simply nothing
// left to extend. The touch is memcached's native one and applies to whatever
// it hits, including an entry kept stale by Invalidate, so a revocation that
// must stick goes through Delete.
func (c *Client) Touch(ctx context.Context, key string, ttl time.Duration) error {
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return err
	}
	touch := baseReadOptions()
	touch.MetadataOnly = true
	touch.Touch = &expiration
	_, err = c.doGet(ctx, key, touch)
	return err
}

// ItemInfo is the read-only metadata returned by Inspect.
type ItemInfo struct {
	// TTL is the remaining lifetime. A negative value means the item never
	// expires.
	TTL time.Duration
	// Size is the stored value's size in bytes.
	Size int
	// LastAccess is the time since the item was last read or written.
	LastAccess time.Duration
	// HitBefore reports whether the item was ever hit since it was stored.
	HitBefore bool
}

// Inspect returns an item's metadata without transferring its value or
// bumping its LRU position. It is an observability tool; branching business
// logic on metadata is not a supported pattern.
func (c *Client) Inspect(ctx context.Context, key string) (ItemInfo, bool, error) {
	result, err := c.doGet(ctx, key, MetaGetOptions{
		MetadataOnly:     true,
		ReturnCAS:        true,
		ReturnTTL:        true,
		ReturnSize:       true,
		ReturnLastAccess: true,
		ReturnHitBefore:  true,
		NoLRUBump:        true,
	})
	if err != nil {
		return ItemInfo{}, false, err
	}
	if result.Status != GetHit {
		return ItemInfo{}, false, nil
	}
	info := ItemInfo{}
	if result.Metadata.TTL != nil {
		info.TTL = time.Duration(*result.Metadata.TTL) * time.Second
	}
	if result.Metadata.Size != nil {
		info.Size = int(*result.Metadata.Size)
	}
	if result.Metadata.LastAccess != nil {
		info.LastAccess = time.Duration(*result.Metadata.LastAccess) * time.Second
	}
	if result.Metadata.HitBefore != nil {
		info.HitBefore = *result.Metadata.HitBefore
	}
	return info, true, nil
}
