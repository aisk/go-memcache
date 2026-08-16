package memcache

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"time"
)

// DialContextFunc opens a server connection and must be safe for concurrent use.
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

type config struct {
	servers              []string
	network              string
	dial                 DialContextFunc
	dialTimeout          time.Duration
	ioTimeout            time.Duration
	idleTimeout          time.Duration
	maxIdle              int
	maxItemSize          int
	router               Router
	copyServersForRouter bool

	defaultRefreshAhead *time.Duration
	degrade             bool
	onError             func(error)
}

func defaultConfig(server string) config {
	d := &net.Dialer{}
	return config{
		servers:     []string{server},
		network:     "tcp",
		dial:        d.DialContext,
		dialTimeout: time.Second,
		ioTimeout:   time.Second,
		idleTimeout: 90 * time.Second,
		maxIdle:     23,
		maxItemSize: 1024 * 1024,
		router:      RendezvousRouter{},
	}
}

// Option configures the client at construction time. Engine options such as
// WithTimeout implement only this interface; policy options additionally
// implement PolicyOption.
type Option interface{ applyOption(*config) error }

// PolicyOption is an Option that declares a client-wide policy default, such
// as RefreshAhead, Degrade, or OnError. Engine options are not PolicyOptions.
type PolicyOption interface {
	Option
	policyOption()
}

// The per-verb option interfaces below make option placement a compile-time
// property: an option is accepted by a verb only when it implements that
// verb's interface. Each interface carries its own unexported method so the
// interfaces stay structurally distinct.

// GetOption modifies a single Get or GetMany call.
type GetOption interface{ applyGet(*callPolicy) }

// FetchOption modifies a Fetch call.
type FetchOption interface{ applyFetch(*callPolicy) }

// callPolicy resolves per-call options against New-level policy defaults.
type callPolicy struct {
	refreshAhead *time.Duration
	touch        *time.Duration
}

func (c *config) callPolicy() callPolicy {
	return callPolicy{refreshAhead: c.defaultRefreshAhead}
}

// Forever stores without expiration. TTL is a positional parameter on every
// verb that needs one, so "never expire" is always a visible choice at the
// call site, never a silent fallback.
const Forever time.Duration = 0

// resolveTTL validates a positional TTL. Zero (Forever) stores without
// expiration; a negative duration is always a mistake. On Incr, Decr,
// Append, and Prepend the TTL applies only when the call auto-creates the
// key; it never extends an existing key's lifetime.
func resolveTTL(ttl time.Duration) (Expiration, error) {
	if ttl < 0 {
		return 0, fmt.Errorf("memcache: TTL must not be negative")
	}
	return ExpiresIn(ttl), nil
}

type touchOption time.Duration

func (o touchOption) applyGet(p *callPolicy) { p.touch = ptr(time.Duration(o)) }

// Touch makes a read slide each hit's expiration to ttl in the same protocol
// command, which turns Get into the read half of session renewal. The slide
// is memcached's native touch and is blind: it extends whatever the read
// hits, including an entry kept stale by Invalidate, so a revocation that
// must stick goes through Delete. Touch(Forever) removes the expiration.
func Touch(ttl time.Duration) GetOption { return touchOption(ttl) }

type refreshAheadOption time.Duration

func (o refreshAheadOption) applyOption(c *config) error {
	if o <= 0 {
		return fmt.Errorf("memcache: RefreshAhead must be positive")
	}
	c.defaultRefreshAhead = ptr(time.Duration(o))
	return nil
}
func (o refreshAheadOption) policyOption()            {}
func (o refreshAheadOption) applyFetch(p *callPolicy) { p.refreshAhead = ptr(time.Duration(o)) }

// RefreshAhead makes Fetch refresh a value in the background once its
// remaining TTL enters the window, so no reader ever pays the recompute
// latency. Meaningful only on Fetch, or as a client-wide default.
func RefreshAhead(d time.Duration) interface {
	PolicyOption
	FetchOption
} {
	return refreshAheadOption(d)
}

type degradeOption bool

func (o degradeOption) applyOption(c *config) error { c.degrade = bool(o); return nil }
func (o degradeOption) policyOption()               {}

// Degrade selects the failure policy for the whole client. When enabled,
// reads report an infrastructure failure as a miss and unconditional writes
// give up silently; every absorbed error still reaches OnError. Verbs whose
// answer feeds a business decision (Add, Replace, Update, Incr, Decr, Take)
// keep returning errors, and an AmbiguousWriteError is never absorbed.
// The default is to return every error.
func Degrade(on bool) PolicyOption { return degradeOption(on) }

type onErrorOption func(error)

func (o onErrorOption) applyOption(c *config) error {
	if o == nil {
		return fmt.Errorf("memcache: nil OnError hook")
	}
	c.onError = o
	return nil
}
func (o onErrorOption) policyOption() {}

// OnError installs the observability hook for failures that never reach a
// caller: errors absorbed by Degrade, background loader failures, and Fetch
// write-back failures. The hook must be safe for concurrent use and must not
// block.
func OnError(hook func(error)) PolicyOption { return onErrorOption(hook) }

type optionFunc func(*config) error

func (f optionFunc) applyOption(c *config) error { return f(c) }

// Router selects a server index for a key. Implementations must be safe for
// concurrent use and return an index in [0, len(servers)). Routers supplied
// with WithRouter receive a slice private to each call and may reorder it
// before Pick returns; the selected address is mapped back to its connection
// pool. Implementations must not retain or mutate the slice after Pick returns.
type Router interface {
	Pick(key string, servers []string) int
}

// RendezvousRouter implements highest-random-weight hashing. Routing uses the
// original key bytes and is stable across processes.
type RendezvousRouter struct{}

func (RendezvousRouter) Pick(key string, servers []string) int {
	best, bestHash := 0, uint64(0)
	for i, server := range servers {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(server))
		if sum := h.Sum64(); i == 0 || sum > bestHash {
			best, bestHash = i, sum
		}
	}
	return best
}

// WithRouter supplies the multi-server routing policy.
func WithRouter(router Router) Option {
	return optionFunc(func(c *config) error {
		if router == nil {
			return fmt.Errorf("memcache: nil router")
		}
		c.router = router
		c.copyServersForRouter = true
		return nil
	})
}

// WithServers configures all backends. Keys are routed consistently across
// the list. At least one non-empty address is required.
func WithServers(servers ...string) Option {
	return optionFunc(func(c *config) error {
		c.servers = append([]string(nil), servers...)
		return nil
	})
}

// WithNetwork changes the network passed to the dialer (normally "tcp" or
// "unix").
func WithNetwork(network string) Option {
	return optionFunc(func(c *config) error { c.network = network; return nil })
}

// WithDialer supplies a custom dialer, useful for TLS and tests.
func WithDialer(dial DialContextFunc) Option {
	return optionFunc(func(c *config) error {
		if dial == nil {
			return fmt.Errorf("memcache: nil dialer")
		}
		c.dial = dial
		return nil
	})
}

// WithTimeout sets the total deadline for one command or one backend's batch
// exchange. Context deadlines take precedence when sooner. A zero duration
// disables this client-side timeout.
func WithTimeout(timeout time.Duration) Option {
	return optionFunc(func(c *config) error {
		if timeout < 0 {
			return fmt.Errorf("memcache: timeout must not be negative")
		}
		c.ioTimeout = timeout
		return nil
	})
}

// WithDialTimeout sets the connection establishment timeout.
func WithDialTimeout(timeout time.Duration) Option {
	return optionFunc(func(c *config) error {
		if timeout < 0 {
			return fmt.Errorf("memcache: dial timeout must not be negative")
		}
		c.dialTimeout = timeout
		return nil
	})
}

// WithIdleTimeout bounds how long a pooled connection may sit idle before it
// is discarded and replaced by a fresh dial. Connections silently dropped by
// a restarted server or an intermediary while idle would otherwise surface as
// a spurious error, or as an AmbiguousWriteError on a mutation. Zero disables
// the limit. The default is 90 seconds.
func WithIdleTimeout(timeout time.Duration) Option {
	return optionFunc(func(c *config) error {
		if timeout < 0 {
			return fmt.Errorf("memcache: idle timeout must not be negative")
		}
		c.idleTimeout = timeout
		return nil
	})
}

// WithMaxIdleConns sets the number of idle connections retained per server.
// Active connections are not capped. Zero disables pooling.
func WithMaxIdleConns(max int) Option {
	return optionFunc(func(c *config) error {
		if max < 0 {
			return fmt.Errorf("memcache: max idle connections must not be negative")
		}
		c.maxIdle = max
		return nil
	})
}

// WithMaxItemSize rejects larger outgoing and incoming values. Zero disables
// the limit. The default is 1 MiB, matching a default memcached server.
func WithMaxItemSize(max int) Option {
	return optionFunc(func(c *config) error {
		if max < 0 {
			return fmt.Errorf("memcache: max item size must not be negative")
		}
		c.maxItemSize = max
		return nil
	})
}
