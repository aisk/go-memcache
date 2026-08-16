package memcache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var errNilOption = errors.New("memcache: nil option")

// errEmptyValue enforces the zero-byte rule: memcached represents lease
// placeholders as zero-byte items, so scenario-layer writes reject empty
// values and scenario-layer reads fold them into a miss.
var errEmptyValue = errors.New("memcache: empty values are reserved as lease placeholders; store a non-empty encoding")

// sceneGet executes one scenario-layer read. Usage errors surface as-is;
// infrastructure failures are folded into a miss when Degrade is enabled.
// An accidental stale-recache win is handed back to the server.
func (c *Client) sceneGet(ctx context.Context, key string, options GetOptions) (GetResult, error) {
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

// sceneReadOptions is the flag set for plain scenario reads. CAS and TTL are
// requested so an accidental stale-recache win can be handed back.
func sceneReadOptions() GetOptions {
	return GetOptions{ReturnCAS: true, ReturnTTL: true}
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
	options := DeleteOptions{Invalidate: true, CompareCAS: cas}
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

// sceneStore executes one scenario-layer write and reports its outcome.
func (c *Client) sceneStore(ctx context.Context, key string, value []byte, options SetOptions) (MutationStatus, error) {
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

// Get reads a value. A miss is a normal answer, not an error: ok reports
// presence, err reports infrastructure failure, and the two never mix. A
// value kept stale by Invalidate is returned as an ordinary hit.
func (c *Client) Get(ctx context.Context, key string, options ...GetOption) ([]byte, bool, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return nil, false, errNilOption
		}
		option.applyGet(&policy)
	}
	result, err := c.sceneGet(ctx, key, sceneReadOptions())
	if err != nil {
		return nil, false, err
	}
	if result.Status != GetHit || len(result.Value) == 0 {
		return nil, false, nil
	}
	return result.Value, true, nil
}

// GetMany reads a set of keys in one round trip per backend and returns the
// hits; a miss is expressed by key absence. With Degrade enabled a failing
// backend only removes its own keys from the result.
func (c *Client) GetMany(ctx context.Context, keys []string, options ...GetOption) (map[string][]byte, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return nil, errNilOption
		}
		option.applyGet(&policy)
	}
	operations := make([]Operation, len(keys))
	for i, key := range keys {
		operations[i] = GetOperation{Key: key, Options: sceneReadOptions()}
	}
	results, err := c.batch(ctx, operations)
	if err != nil {
		return nil, err
	}
	found := make(map[string][]byte, len(keys))
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
		if len(result.Get.Value) > 0 {
			found[keys[i]] = result.Get.Value
		}
	}
	return found, firstErr
}

// GetTouch reads a value and slides its expiration to ttl. It is the read
// half of session renewal. The slide is a CAS-gated rewrite of the value just
// read rather than a blind server-side touch: a blind touch would also extend
// an entry marked stale by Invalidate, keeping revoked data alive past its
// grace period. An entry that is missing, stale, or invalidated mid-renewal
// reads as a miss and keeps decaying.
func (c *Client) GetTouch(ctx context.Context, key string, ttl time.Duration, options ...GetTouchOption) ([]byte, bool, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return nil, false, errNilOption
		}
		option.applyGetTouch(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return nil, false, err
	}
	read := sceneReadOptions()
	read.ReturnClientFlags = true
	for range updateAttempts {
		result, err := c.sceneGet(ctx, key, read)
		if err != nil {
			return nil, false, err
		}
		if result.Status != GetHit || result.ValueState != ValueFresh || len(result.Value) == 0 {
			return nil, false, nil
		}
		if result.Metadata.CAS == nil {
			return nil, false, &ProtocolError{Message: "value read omitted requested CAS"}
		}
		write := SetOptions{TTL: expiration, Mode: ModeReplace, CompareCAS: result.Metadata.CAS}
		if result.Metadata.ClientFlags != nil {
			write.ClientFlags = *result.Metadata.ClientFlags
		}
		status, err := c.sceneStore(ctx, key, result.Value, write)
		if err != nil {
			if c.absorb(ctx, err) {
				// The read succeeded; under Degrade the slide is best-effort.
				return result.Value, true, nil
			}
			return nil, false, err
		}
		if status == MutationApplied {
			return result.Value, true, nil
		}
		// The entry changed between read and renewal; read again.
	}
	return nil, false, ErrConflict
}

// Set unconditionally stores a value for ttl. Storing without expiration is
// the explicit choice Forever.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration, options ...SetOption) error {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return errNilOption
		}
		option.applySet(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return errEmptyValue
	}
	status, err := c.sceneStore(ctx, key, value, SetOptions{TTL: expiration})
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
func (c *Client) SetMany(ctx context.Context, mapping map[string][]byte, ttl time.Duration, options ...SetOption) error {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return errNilOption
		}
		option.applySet(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return err
	}
	operations := make([]Operation, 0, len(mapping))
	for key, value := range mapping {
		if len(value) == 0 {
			return fmt.Errorf("memcache: value for %q: %w", key, errEmptyValue)
		}
		operations = append(operations, SetOperation{Key: key, Value: value, Options: SetOptions{TTL: expiration}})
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
// The bool is the scenario's whole answer, so Add keeps it; it is also why
// Degrade never fakes a result here.
func (c *Client) Add(ctx context.Context, key string, value []byte, ttl time.Duration, options ...SetOption) (bool, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return false, errNilOption
		}
		option.applySet(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return false, err
	}
	if len(value) == 0 {
		return false, errEmptyValue
	}
	status, err := c.sceneStore(ctx, key, value, SetOptions{TTL: expiration, Mode: ModeAdd})
	if err != nil {
		return false, err
	}
	return status == MutationApplied, nil
}

// Replace stores only when the key still exists and reports whether it did.
// It is the write half of session renewal: false means the session ended
// mid-request and there is nothing to write back to.
func (c *Client) Replace(ctx context.Context, key string, value []byte, ttl time.Duration, options ...SetOption) (bool, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return false, errNilOption
		}
		option.applySet(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return false, err
	}
	if len(value) == 0 {
		return false, errEmptyValue
	}
	status, err := c.sceneStore(ctx, key, value, SetOptions{TTL: expiration, Mode: ModeReplace})
	if err != nil {
		return false, err
	}
	return status == MutationApplied, nil
}

// updateAttempts bounds the optimistic concurrency loops in Update and Drain.
const updateAttempts = 8

// Update atomically transforms a value and stores the result for ttl: read
// with version, apply fn, write back only if unchanged, retry on conflict.
// Version tokens never appear in user code. On a miss fn receives (nil,
// false); returning an error from fn aborts the whole operation without
// writing. fn may run multiple times and must be pure. A value kept stale by
// Invalidate is treated as a miss: fn transforms rather than recomputes, and
// transforming invalidated data would silently launder it back to fresh.
func (c *Client) Update(ctx context.Context, key string, ttl time.Duration, fn func(current []byte, found bool) ([]byte, error), options ...UpdateOption) ([]byte, error) {
	if fn == nil {
		return nil, fmt.Errorf("memcache: Update requires a transform function")
	}
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return nil, errNilOption
		}
		option.applyUpdate(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return nil, err
	}
	readOptions := sceneReadOptions()
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
		writeOptions := SetOptions{TTL: expiration}
		if compareCAS != nil {
			writeOptions.CompareCAS = compareCAS
		} else {
			writeOptions.Mode = ModeAdd
		}
		status, err := c.sceneStore(ctx, key, next, writeOptions)
		if err != nil {
			return nil, err
		}
		if status == MutationApplied {
			return next, nil
		}
	}
	return nil, ErrConflict
}

// Delete removes a key. Deleting an absent key is a success: the goal state
// already holds.
func (c *Client) Delete(ctx context.Context, key string) error {
	command, err := buildDelete(key, DeleteOptions{})
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
// Use Delete when the old value must not be served for even a second.
func (c *Client) Invalidate(ctx context.Context, key string, grace time.Duration) error {
	if grace <= 0 {
		return fmt.Errorf("memcache: Invalidate grace must be positive")
	}
	staleFor := ExpiresIn(grace)
	command, err := buildDelete(key, DeleteOptions{Invalidate: true, StaleFor: &staleFor})
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

// Touch extends a key's TTL without transferring its value. A missing key is
// not an error; there is simply nothing left to extend. An entry marked stale
// by Invalidate is left alone: extending it would keep revoked data alive, so
// it keeps decaying toward its miss.
func (c *Client) Touch(ctx context.Context, key string, ttl time.Duration) error {
	if _, err := resolveTTL(ttl); err != nil {
		return err
	}
	probe := sceneReadOptions()
	probe.MetadataOnly = true
	result, err := c.sceneGet(ctx, key, probe)
	if err != nil {
		return err
	}
	if result.Status != GetHit || result.ValueState != ValueFresh {
		return nil
	}
	// The protocol has no CAS-gated touch, so the extension is a second
	// command: an Invalidate can still slip in between the two, but the
	// exposure shrinks from every call to one round-trip window.
	expiration := ExpiresIn(ttl)
	touch := sceneReadOptions()
	touch.MetadataOnly = true
	touch.Touch = &expiration
	_, err = c.sceneGet(ctx, key, touch)
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
	result, err := c.sceneGet(ctx, key, GetOptions{
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

// counter is the shared implementation of Incr and Decr.
func (c *Client) counter(ctx context.Context, key string, delta uint64, decrement bool, options []CounterOption) (uint64, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return 0, errNilOption
		}
		option.applyCounter(&policy)
	}
	window, err := policy.resolveWindow()
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
	arithmetic := ArithmeticOptions{Delta: delta, Decrement: decrement, Initial: &initial, InitialTTL: &window}
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

// Incr adds delta to a decimal counter, creating it inside the resolved
// Window on a miss so the first request counts as delta. The window is fixed
// at creation; later increments never extend it. The result feeds business
// decisions, so Degrade never fakes one: infrastructure failures surface.
func (c *Client) Incr(ctx context.Context, key string, delta uint64, options ...CounterOption) (uint64, error) {
	return c.counter(ctx, key, delta, false, options)
}

// Decr subtracts delta from a decimal counter, saturating at zero. A miss
// creates the counter at zero inside the resolved Window.
func (c *Client) Decr(ctx context.Context, key string, delta uint64, options ...CounterOption) (uint64, error) {
	return c.counter(ctx, key, delta, true, options)
}

// stream is the shared implementation of Append and Prepend.
func (c *Client) stream(ctx context.Context, key string, fragment []byte, mode StoreMode, options []StreamOption) error {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return errNilOption
		}
		option.applyStream(&policy)
	}
	window, err := policy.resolveWindow()
	if err != nil {
		return err
	}
	if len(fragment) == 0 {
		return errEmptyValue
	}
	status, err := c.sceneStore(ctx, key, fragment, SetOptions{Mode: mode, VivifyTTL: &window})
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

// Append adds a fragment to the end of a byte-stream buffer, creating the
// buffer inside the resolved Window on a miss. Fragments bypass any value
// encoding: the buffer is raw bytes by design.
func (c *Client) Append(ctx context.Context, key string, fragment []byte, options ...StreamOption) error {
	return c.stream(ctx, key, fragment, ModeAppend, options)
}

// Prepend adds a fragment to the front of a byte-stream buffer, creating the
// buffer inside the resolved Window on a miss.
func (c *Client) Prepend(ctx context.Context, key string, fragment []byte, options ...StreamOption) error {
	return c.stream(ctx, key, fragment, ModePrepend, options)
}

// Peek reads a byte-stream buffer without clearing it.
func (c *Client) Peek(ctx context.Context, key string) ([]byte, bool, error) {
	result, err := c.sceneGet(ctx, key, sceneReadOptions())
	if err != nil {
		return nil, false, err
	}
	if result.Status != GetHit || len(result.Value) == 0 {
		return nil, false, nil
	}
	return result.Value, true, nil
}

// Drain atomically takes a byte-stream buffer and clears it: read with
// version, delete only if unchanged, retry when a concurrent append slipped
// in between. There is no window in which appended events can be lost. A nil
// result means the buffer was empty; a miss and an empty buffer are
// deliberately the same answer. Drained data feeds a consumer, so Degrade
// never absorbs failures here.
func (c *Client) Drain(ctx context.Context, key string) ([]byte, error) {
	readOptions := sceneReadOptions()
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
		command, err = buildDelete(key, DeleteOptions{CompareCAS: result.Metadata.CAS})
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
		// A CAS mismatch means new events arrived after the read; a vanished
		// key means someone else cleared it. Either way, read again.
	}
	return nil, ErrConflict
}
