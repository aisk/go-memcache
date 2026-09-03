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

// Fetch returns the cached value or computes it exactly once, storing the
// computed value for ttl. On a miss it takes a server-side lease so that
// across processes and goroutines a single loader runs while everyone else
// waits for its result; near expiry (inside RefreshAhead) or during an
// Invalidate grace period it returns the current value immediately and
// recomputes in the background. Fetch never fails
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
//
// The loader's value is encoded with the client's codec before it is
// stored, and every caller, the winner included, receives the decoded stored
// form; a []byte value passes through untouched.
func (c *Client) Fetch[T any](ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (T, error), options ...FetchOption) (T, error) {
	var zero T
	if loader == nil {
		return zero, fmt.Errorf("memcache: Fetch requires a loader")
	}
	raw, err := c.fetch(ctx, key, ttl, func(loaderCtx context.Context) ([]byte, error) {
		value, err := loader(loaderCtx)
		if err != nil {
			return nil, err
		}
		return c.encode(value)
	}, options...)
	if err != nil {
		return zero, err
	}
	return decode[T](c, raw)
}

// fetchPlan is what a Fetch or FetchMany call resolves from its ttl and
// options before touching the network.
type fetchPlan struct {
	expiration Expiration
	window     time.Duration // bounds a background refresh
	read       MetaGetOptions
}

func (c *Client) planFetch(ttl time.Duration, options []FetchOption) (fetchPlan, error) {
	policy := c.config.callPolicy()
	for _, option := range options {
		if option == nil {
			return fetchPlan{}, errNilOption
		}
		option.applyFetch(&policy)
	}
	expiration, err := resolveTTL(ttl)
	if err != nil {
		return fetchPlan{}, err
	}
	plan := fetchPlan{expiration: expiration, window: fetchLeaseTTL}
	var refreshBefore *Expiration
	if policy.refreshAhead != nil {
		if *policy.refreshAhead <= 0 {
			return fetchPlan{}, fmt.Errorf("memcache: RefreshAhead must be positive")
		}
		// A window at or beyond the TTL would put every write-back already
		// inside it, turning steady reads into a perpetual recompute loop.
		if ttl > 0 && *policy.refreshAhead >= ttl {
			return fetchPlan{}, fmt.Errorf("memcache: RefreshAhead (%v) must be shorter than the TTL (%v)", *policy.refreshAhead, ttl)
		}
		plan.window = *policy.refreshAhead
		refreshBefore = ptr(ExpiresIn(plan.window))
	}
	lease := ExpiresIn(fetchLeaseTTL)
	plan.read = MetaGetOptions{ReturnCAS: true, VivifyTTL: &lease, RefreshBefore: refreshBefore}
	return plan, nil
}

// fetch is Fetch's state machine over stored bytes.
func (c *Client) fetch(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) ([]byte, error), options ...FetchOption) ([]byte, error) {
	plan, err := c.planFetch(ttl, options)
	if err != nil {
		return nil, err
	}
	expiration, refreshWindow, readOptions := plan.expiration, plan.window, plan.read
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
				c.spawnRefresh(key, *result.Metadata.CAS, expiration, refreshWindow, loader)
			}
			return result.Value, nil

		case result.Status == GetMiss && result.Lease == LeaseGranted && result.Metadata.CAS != nil:
			// This caller won the vivify lease: it recomputes synchronously
			// (there is nothing to return otherwise) and same-process
			// callers wait on its result.
			return c.leadLoad(ctx, key, *result.Metadata.CAS, expiration, loader)

		case result.Status == GetHit && result.Lease != LeaseBusy:
			// A genuinely stored zero-byte item, or an empty entry whose
			// stale-recache token this read just won. No other caller is
			// coming to rewrite it, so waiting would pay the full backoff on
			// every Fetch forever; replace it through its CAS instead.
			if result.Metadata.CAS != nil {
				return c.leadLoad(ctx, key, *result.Metadata.CAS, expiration, loader)
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
	command, err := buildDelete(key, MetaDeleteOptions{CompareCAS: &cas})
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
	command, err := buildSet(key, value, MetaSetOptions{TTL: ttl, CompareCAS: &cas})
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

// FetchMany is Fetch over a set of keys. It reads every key in one round
// trip per backend, returns the cached values, and computes the rest with a
// single loader call, storing what the loader returns for ttl. Each key is
// coordinated exactly as Fetch coordinates one: a miss is leased to one
// caller across processes, same-process callers share that caller's result,
// and a key inside RefreshAhead or an Invalidate grace period returns its
// current value while a background loader call recomputes it.
//
// The loader receives the keys that need computing and returns what it
// found; a key it leaves out is absent from the result too, as it would be
// after a miss at the source, and its lease is released so the next call
// re-elects. Keys outside missing are ignored. A loader error fails the
// whole call, as Fetch's does. The loader runs on a client-owned context
// for the same reasons as Fetch's, and every value comes back decoded from
// its stored form.
func (c *Client) FetchMany[T any](ctx context.Context, keys []string, ttl time.Duration, loader func(ctx context.Context, missing []string) (map[string]T, error), options ...FetchOption) (map[string]T, error) {
	if loader == nil {
		return nil, fmt.Errorf("memcache: FetchMany requires a loader")
	}
	raw, err := c.fetchMany(ctx, keys, ttl, func(loaderCtx context.Context, missing []string) (map[string][]byte, error) {
		values, err := loader(loaderCtx, missing)
		if err != nil {
			return nil, err
		}
		encoded := make(map[string][]byte, len(values))
		for key, value := range values {
			data, err := c.encode(value)
			if err != nil {
				return nil, err
			}
			encoded[key] = data
		}
		return encoded, nil
	}, options...)
	if err != nil {
		return nil, err
	}
	found := make(map[string]T, len(raw))
	var firstErr error
	for key, data := range raw {
		value, err := decode[T](c, data)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		found[key] = value
	}
	return found, firstErr
}

// leaseHolder is a key together with the CAS token of the read that won its
// lease; a write-back or release is conditioned on that token.
type leaseHolder struct {
	key string
	cas uint64
}

// fetchMany is FetchMany's state machine over stored bytes. It classifies
// every key the way fetch classifies one, then makes one loader call for
// all the keys this caller has to compute: leased keys are written back,
// unleased ones (Degrade, a lossy miss, an exhausted wait) are not.
func (c *Client) fetchMany(ctx context.Context, keys []string, ttl time.Duration, loader func(context.Context, []string) (map[string][]byte, error), options ...FetchOption) (map[string][]byte, error) {
	plan, err := c.planFetch(ttl, options)
	if err != nil {
		return nil, err
	}
	found := make(map[string][]byte, len(keys))
	pending := uniqueKeys(keys)
	if len(pending) == 0 {
		return found, nil
	}

	var lead, refresh []leaseHolder
	var local []string
	for attempt := 0; ; attempt++ {
		operations := make([]Operation, len(pending))
		for i, key := range pending {
			operations[i] = GetOperation{Key: key, Options: plan.read}
		}
		results, err := c.batch(ctx, operations)
		if err != nil {
			return nil, err
		}
		var busy []string
		for i, result := range results {
			key := pending[i]
			if result.Err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(result.Err, ctxErr) {
					return nil, result.Err
				}
				if c.config.degrade {
					c.reportError(result.Err)
					local = append(local, key)
					continue
				}
				return nil, result.Err
			}
			r := result.Get
			switch {
			case r.Status == GetHit && len(r.Value) > 0:
				found[key] = r.Value
				if r.Lease == LeaseGranted && r.Metadata.CAS != nil {
					refresh = append(refresh, leaseHolder{key, *r.Metadata.CAS})
				}
			case r.Status == GetMiss && r.Lease == LeaseGranted && r.Metadata.CAS != nil:
				lead = append(lead, leaseHolder{key, *r.Metadata.CAS})
			case r.Status == GetHit && r.Lease != LeaseBusy:
				if r.Metadata.CAS != nil {
					lead = append(lead, leaseHolder{key, *r.Metadata.CAS})
				} else {
					local = append(local, key)
				}
			case r.Status == GetMiss:
				local = append(local, key)
			default:
				busy = append(busy, key)
			}
		}
		if len(busy) == 0 {
			break
		}
		// Another caller holds these leases. Same-process waiters share the
		// winner's pending result; the rest are re-read after a short wait
		// until the schedule runs out, then computed locally.
		var remaining []string
		for _, key := range busy {
			value, err, joined := c.joinFlight(ctx, key)
			switch {
			case !joined:
				remaining = append(remaining, key)
			case err != nil:
				return nil, err
			case len(value) > 0:
				found[key] = value
			}
		}
		if len(remaining) == 0 {
			break
		}
		if attempt >= len(fetchWaitBackoff) {
			local = append(local, remaining...)
			break
		}
		select {
		case <-time.After(fetchWaitBackoff[attempt]):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		pending = remaining
	}

	if len(refresh) > 0 {
		c.spawnRefreshMany(refresh, plan.expiration, plan.window, loader)
	}
	if len(lead) == 0 && len(local) == 0 {
		return found, nil
	}
	computed, err := c.runFlights(ctx, lead, local, loader)
	// Settle every lease this caller won, whether its loader ran here or the
	// key was joined from another caller's flight: a value is written back
	// under the winning CAS, anything else releases the placeholder so the
	// next call re-elects instead of waiting out the lease.
	for _, holder := range lead {
		value, present := computed[holder.key]
		switch {
		case err == nil && len(value) > 0:
			c.writeBack(holder.key, value, holder.cas, plan.expiration)
		default:
			if err == nil && present && value != nil {
				c.reportError(fmt.Errorf("memcache: fetch loader for %q returned an empty value, which cannot be cached", holder.key))
			}
			c.releaseLease(holder.key, holder.cas)
		}
	}
	if err != nil {
		return nil, err
	}
	for key, value := range computed {
		if len(value) > 0 {
			found[key] = value
		}
	}
	return found, nil
}

// runFlights is runFlight over a set of keys with one loader call. Keys
// already in flight in this process are joined rather than recomputed, so a
// concurrent Fetch or FetchMany on the same key still runs one loader per
// key per process. The result holds every key the loader returned or a
// joined flight produced, empty values included, so the caller can settle
// leases; the first loader error, own or joined, is returned with it.
func (c *Client) runFlights(ctx context.Context, lead []leaseHolder, local []string, load func(context.Context, []string) (map[string][]byte, error)) (map[string][]byte, error) {
	type joined struct {
		key    string
		flight *fetchFlight
	}
	own := make([]string, 0, len(lead)+len(local))
	var joins []joined
	flights := make(map[string]*fetchFlight)
	register := func(key string) {
		if existing := c.fetchFlights[key]; existing != nil {
			joins = append(joins, joined{key, existing})
			return
		}
		flight := &fetchFlight{done: make(chan struct{})}
		c.fetchFlights[key] = flight
		flights[key] = flight
		own = append(own, key)
	}
	c.fetchMu.Lock()
	for _, holder := range lead {
		register(holder.key)
	}
	for _, key := range local {
		register(key)
	}
	c.fetchMu.Unlock()

	values := make(map[string][]byte, len(own)+len(joins))
	var firstErr error
	if len(own) > 0 {
		loaderCtx, cancel := c.detachedContext(ctx)
		loaded, err := load(loaderCtx, own)
		cancel()
		c.fetchMu.Lock()
		for key, flight := range flights {
			flight.value, flight.err = loaded[key], err
			delete(c.fetchFlights, key)
		}
		c.fetchMu.Unlock()
		for _, flight := range flights {
			close(flight.done)
		}
		firstErr = err
		for _, key := range own {
			if value, returned := loaded[key]; returned {
				values[key] = value
			}
		}
	}
	for _, j := range joins {
		select {
		case <-j.flight.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if j.flight.err != nil {
			if firstErr == nil {
				firstErr = j.flight.err
			}
			continue
		}
		values[j.key] = j.flight.value
	}
	return values, firstErr
}

// spawnRefreshMany is spawnRefresh for the keys a FetchMany read won refresh
// leases on: one background loader call recomputes all of them, and each
// result is written back under its own CAS. Keys already refreshing in this
// process are left to that refresh.
func (c *Client) spawnRefreshMany(holders []leaseHolder, ttl Expiration, window time.Duration, loader func(context.Context, []string) (map[string][]byte, error)) {
	c.fetchMu.Lock()
	if c.rootCtx.Err() != nil {
		c.fetchMu.Unlock()
		return
	}
	var mine []leaseHolder
	for _, holder := range holders {
		if _, active := c.refreshing[holder.key]; active {
			continue
		}
		c.refreshing[holder.key] = struct{}{}
		mine = append(mine, holder)
	}
	c.fetchMu.Unlock()
	if len(mine) == 0 {
		return
	}
	go func() {
		defer func() {
			c.fetchMu.Lock()
			for _, holder := range mine {
				delete(c.refreshing, holder.key)
			}
			c.fetchMu.Unlock()
		}()
		keys := make([]string, len(mine))
		for i, holder := range mine {
			keys[i] = holder.key
		}
		refreshCtx, cancel := context.WithTimeout(c.rootCtx, window)
		defer cancel()
		values, err := loader(refreshCtx, keys)
		if err != nil {
			c.reportError(fmt.Errorf("memcache: fetch background refresh of %d keys: %w", len(keys), err))
			return
		}
		for _, holder := range mine {
			value := values[holder.key]
			if len(value) == 0 {
				c.reportError(fmt.Errorf("memcache: fetch background refresh of %q produced no value, which cannot be cached", holder.key))
				continue
			}
			c.writeBack(holder.key, value, holder.cas, ttl)
		}
	}()
}

// uniqueKeys drops repeated keys while keeping first-occurrence order.
func uniqueKeys(keys []string) []string {
	unique := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}
