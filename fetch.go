package memcache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// fetchLeaseTTL bounds a miss-path lease: the zero-byte placeholder created
// by vivify lives this long, so a crashed winner's exclusive right to
// recompute expires on its own and a later request re-elects.
const fetchLeaseTTL = 30 * time.Second

// fetchWaitBackoff paces a loser's cross-process wait for another process's
// winner. When the schedule is exhausted the caller computes locally without
// writing back.
var fetchWaitBackoff = []time.Duration{
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
}

// fetchFlight is the in-process merge point for one key's miss-path load:
// the winner publishes its result here and every same-process waiter shares
// it, so a process runs at most one loader per key.
type fetchFlight struct {
	done  chan struct{}
	value []byte
	err   error
}

// Fetch returns the cached value or computes it exactly once. On a miss it
// takes a server-side lease so that across processes and goroutines a single
// loader runs while everyone else waits for its result; near expiry (inside
// RefreshAhead) or during an Invalidate grace period it returns the current
// value immediately and recomputes in the background. Fetch never fails
// because coordination failed: every path ends in a value, the loader's own
// error, or the caller's context error, and write-back failures only reach
// OnError. Paths that compute without a lease (Degrade, a lease held
// elsewhere) still merge through the in-process flight map, so a process runs
// at most one loader per key.
//
// The loader does not run on the calling context: a background refresh
// outlives its caller, and a miss-path result is shared by every waiter in
// the process, so one short-deadline caller must not cancel it for everyone.
// The loader receives a context owned by the client (carrying the winning
// caller's deadline on the miss path) and must not rely on request-scoped
// values.
func (c *Client) Fetch(ctx context.Context, key string, loader func(context.Context) ([]byte, error), options ...FetchOption) ([]byte, error) {
	if loader == nil {
		return nil, fmt.Errorf("memcache: Fetch requires a loader")
	}
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return nil, errNilOption
		}
		option.applyFetch(&policy)
	}
	ttl, err := policy.resolveTTL()
	if err != nil {
		return nil, err
	}
	refreshWindow := fetchLeaseTTL
	var refreshBefore *Expiration
	if policy.refreshAhead != nil {
		if *policy.refreshAhead <= 0 {
			return nil, fmt.Errorf("memcache: RefreshAhead must be positive")
		}
		// A window at or beyond the TTL would put every write-back already
		// inside it, turning steady reads into a perpetual recompute loop.
		if policy.ttl != nil && *policy.ttl > 0 && *policy.refreshAhead >= *policy.ttl {
			return nil, fmt.Errorf("memcache: RefreshAhead (%v) must be shorter than the TTL (%v)", *policy.refreshAhead, *policy.ttl)
		}
		refreshWindow = *policy.refreshAhead
		refreshBefore = ptr(ExpiresIn(refreshWindow))
	}
	lease := ExpiresIn(fetchLeaseTTL)
	readOptions := GetOptions{ReturnCAS: true, VivifyTTL: &lease, RefreshBefore: refreshBefore}
	command, err := buildGet(key, readOptions)
	if err != nil {
		return nil, err
	}

	for attempt := 0; ; attempt++ {
		wire, err := c.executeMeta(ctx, command)
		var result GetResult
		if err == nil {
			result, err = semanticGet(key, readOptions, wire)
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				// The caller gave up; running the loader for it would only
				// dress a context error up as a loader result.
				return nil, err
			}
			if c.config.degrade {
				// Cache outage is not a site outage: compute locally, skip
				// the cache entirely, and keep the failure observable.
				c.reportError(err)
				return c.localCompute(ctx, key, loader)
			}
			return nil, err
		}

		switch {
		case result.Status == GetHit && len(result.Value) > 0:
			// Fresh hit, refresh-ahead win, or stale-grace value. A winner
			// returns the current value immediately and recomputes in the
			// background; blocking it would recreate the latency spike this
			// path exists to remove.
			if result.Lease == LeaseGranted && result.Metadata.CAS != nil {
				c.spawnRefresh(key, *result.Metadata.CAS, ttl, refreshWindow, loader)
			}
			return result.Value, nil

		case result.Status == GetMiss && result.Lease == LeaseGranted && result.Metadata.CAS != nil:
			// This caller won the vivify lease: it recomputes synchronously
			// (there is nothing to return otherwise) and same-process
			// callers wait on its result.
			return c.leadLoad(ctx, key, *result.Metadata.CAS, ttl, loader)

		case result.Status == GetHit && result.Lease != LeaseBusy:
			// A genuinely stored zero-byte item, or an empty entry whose
			// stale-recache token this read just won. No other caller is
			// coming to rewrite it, so waiting would pay the full backoff on
			// every Fetch forever; replace it through its CAS instead.
			if result.Metadata.CAS != nil {
				return c.leadLoad(ctx, key, *result.Metadata.CAS, ttl, loader)
			}
			return c.localCompute(ctx, key, loader)

		case result.Status == GetMiss:
			// A miss without a lease has no coordination to offer; compute
			// without write-back, merged with any same-process computation.
			return c.localCompute(ctx, key, loader)
		}

		// Another caller holds the lease. Same-process waiters share the
		// winner's pending result; cross-process losers wait briefly and
		// retry, then fall back to a merged local computation without
		// write-back.
		if value, err, joined := c.joinFlight(ctx, key); joined {
			return value, err
		}
		if attempt >= len(fetchWaitBackoff) {
			return c.localCompute(ctx, key, loader)
		}
		select {
		case <-time.After(fetchWaitBackoff[attempt]):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// joinFlight waits on this process's in-flight loader for key, if any. A
// waiter respects its own context: on cancellation it stops waiting and
// returns the context error.
func (c *Client) joinFlight(ctx context.Context, key string) ([]byte, error, bool) {
	c.fetchMu.Lock()
	flight := c.fetchFlights[key]
	c.fetchMu.Unlock()
	if flight == nil {
		return nil, nil, false
	}
	select {
	case <-flight.done:
		return flight.value, flight.err, true
	case <-ctx.Done():
		return nil, ctx.Err(), true
	}
}

// runFlight runs load once per key per process: the first caller registers
// the flight and runs it on a detached context, and every concurrent caller
// shares the published result. A caller whose own context ends while waiting
// gets its context error.
func (c *Client) runFlight(ctx context.Context, key string, load func(context.Context) ([]byte, error)) ([]byte, error) {
	flight := &fetchFlight{done: make(chan struct{})}
	c.fetchMu.Lock()
	if existing := c.fetchFlights[key]; existing != nil {
		c.fetchMu.Unlock()
		select {
		case <-existing.done:
			return existing.value, existing.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.fetchFlights[key] = flight
	c.fetchMu.Unlock()

	loaderCtx, cancel := c.detachedContext(ctx)
	value, err := load(loaderCtx)
	cancel()

	flight.value, flight.err = value, err
	c.fetchMu.Lock()
	delete(c.fetchFlights, key)
	c.fetchMu.Unlock()
	close(flight.done)
	return value, err
}

// leadLoad runs the miss-path loader as the elected winner and publishes the
// result to same-process waiters. The loader error is passed through to
// everyone waiting on it; only write-back failures are diverted to OnError.
// When nothing gets written back, the zero-byte item that granted the lease
// is released so the next Fetch can re-elect immediately instead of waiting
// out the placeholder TTL.
func (c *Client) leadLoad(ctx context.Context, key string, cas uint64, ttl Expiration, loader func(context.Context) ([]byte, error)) ([]byte, error) {
	return c.runFlight(ctx, key, func(loaderCtx context.Context) ([]byte, error) {
		value, err := loader(loaderCtx)
		switch {
		case err != nil:
			c.releaseLease(key, cas)
		case len(value) == 0:
			c.reportError(fmt.Errorf("memcache: fetch loader for %q returned an empty value, which cannot be cached", key))
			c.releaseLease(key, cas)
		default:
			c.writeBack(key, value, cas, ttl)
		}
		return value, err
	})
}

// localCompute runs the loader without write-back for the paths where this
// caller holds no lease (Degrade, a lossy miss, an exhausted cross-process
// wait). Merging through the flight map keeps the one-loader-per-key-per-
// process guarantee exactly when the origin is most exposed.
func (c *Client) localCompute(ctx context.Context, key string, loader func(context.Context) ([]byte, error)) ([]byte, error) {
	return c.runFlight(ctx, key, loader)
}

// releaseLease deletes the zero-byte item whose lease this caller consumed
// but could not repay with a write-back. The CAS guard means a concurrent
// rewrite wins and the delete quietly stands down; only transport failures
// are worth reporting.
func (c *Client) releaseLease(key string, cas uint64) {
	command, err := buildDelete(key, DeleteOptions{CompareCAS: &cas})
	if err == nil {
		var wire RawResponse
		if wire, err = c.executeMeta(c.rootCtx, command); err == nil {
			_, err = deleteStatus(wire.Code)
		}
	}
	if err != nil {
		c.reportError(fmt.Errorf("memcache: fetch releasing the lease for %q: %w", key, err))
	}
}

// detachedContext derives the winner's loader context from the client's root
// context so a single caller's cancellation cannot cancel a shared result,
// while still honoring the winning caller's deadline.
func (c *Client) detachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(c.rootCtx, deadline)
	}
	return context.WithCancel(c.rootCtx)
}

// spawnRefresh recomputes a value in the background after a refresh-ahead or
// stale-grace lease win. The goroutine is bounded by the window that
// triggered it and by client Close; per key, at most one refresh runs at a
// time in this process.
func (c *Client) spawnRefresh(key string, cas uint64, ttl Expiration, window time.Duration, loader func(context.Context) ([]byte, error)) {
	c.fetchMu.Lock()
	if _, active := c.refreshing[key]; active || c.rootCtx.Err() != nil {
		c.fetchMu.Unlock()
		return
	}
	c.refreshing[key] = struct{}{}
	c.fetchMu.Unlock()
	go func() {
		defer func() {
			c.fetchMu.Lock()
			delete(c.refreshing, key)
			c.fetchMu.Unlock()
		}()
		refreshCtx, cancel := context.WithTimeout(c.rootCtx, window)
		defer cancel()
		value, err := loader(refreshCtx)
		if err != nil {
			c.reportError(fmt.Errorf("memcache: fetch background refresh of %q: %w", key, err))
			return
		}
		if len(value) == 0 {
			c.reportError(fmt.Errorf("memcache: fetch loader for %q returned an empty value, which cannot be cached", key))
			return
		}
		c.writeBack(key, value, cas, ttl)
	}()
}

// writeBack stores a recomputed value conditioned on the CAS token from the
// winning read. Background recomputation widens the gap between "loader
// started" and "write lands": if the key was deleted, replaced, or
// invalidated again inside that gap, an unconditional write would resurrect
// dead data, so a rejected write is abandoned. Write-back failures never
// change what Fetch returns; they are observability events.
func (c *Client) writeBack(key string, value []byte, cas uint64, ttl Expiration) {
	command, err := buildSet(key, value, SetOptions{TTL: ttl, CompareCAS: &cas})
	if err == nil {
		var wire RawResponse
		wire, err = c.executeMeta(c.rootCtx, command)
		if err == nil {
			var status MutationStatus
			status, err = storeStatus(wire.Code, ModeSet)
			if err == nil && status != MutationApplied {
				err = fmt.Errorf("memcache: fetch write-back of %q abandoned: the entry changed during recomputation", key)
			}
		}
	}
	if err != nil {
		c.reportError(fmt.Errorf("memcache: fetch write-back of %q: %w", key, err))
	}
}
