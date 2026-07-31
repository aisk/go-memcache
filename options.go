package memcache

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"time"
)

// Context is an alias used by generic helper functions whose method-shaped
// equivalent cannot yet be expressed in Go.
type Context = context.Context

// DialContextFunc opens a server connection.
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

type config struct {
	servers     []string
	network     string
	dial        DialContextFunc
	dialTimeout time.Duration
	ioTimeout   time.Duration
	maxIdle     int
	maxItemSize int
	codec       Codec
	router      Router
}

// Option configures a Client.
type Option func(*config) error

func defaultConfig(server string) config {
	d := &net.Dialer{Timeout: time.Second}
	return config{
		servers:     []string{server},
		network:     "tcp",
		dial:        d.DialContext,
		dialTimeout: time.Second,
		ioTimeout:   time.Second,
		maxIdle:     23,
		maxItemSize: 1024 * 1024,
		codec:       JSONCodec{},
		router:      RendezvousRouter{},
	}
}

// Router selects a server index for a key. Implementations must be safe for
// concurrent use and return an index in [0, len(servers)).
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
	return func(c *config) error {
		if router == nil {
			return fmt.Errorf("memcache: nil router")
		}
		c.router = router
		return nil
	}
}

// WithServers configures all backends. Keys are routed consistently across
// the list. At least one non-empty address is required.
func WithServers(servers ...string) Option {
	return func(c *config) error {
		c.servers = append([]string(nil), servers...)
		return nil
	}
}

// WithNetwork changes the network passed to the dialer (normally "tcp" or
// "unix").
func WithNetwork(network string) Option {
	return func(c *config) error { c.network = network; return nil }
}

// WithDialer supplies a custom dialer, useful for TLS and tests.
func WithDialer(dial DialContextFunc) Option {
	return func(c *config) error {
		if dial == nil {
			return fmt.Errorf("memcache: nil dialer")
		}
		c.dial = dial
		return nil
	}
}

// WithTimeout sets the per-I/O timeout. Context deadlines take precedence
// when sooner. A zero duration disables this client-side timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout < 0 {
			return fmt.Errorf("memcache: timeout must not be negative")
		}
		c.ioTimeout = timeout
		return nil
	}
}

// WithDialTimeout sets the connection establishment timeout.
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout < 0 {
			return fmt.Errorf("memcache: dial timeout must not be negative")
		}
		c.dialTimeout = timeout
		return nil
	}
}

// WithMaxIdleConns sets the number of idle connections retained per server.
// Active connections are not capped. Zero disables pooling.
func WithMaxIdleConns(max int) Option {
	return func(c *config) error {
		if max < 0 {
			return fmt.Errorf("memcache: max idle connections must not be negative")
		}
		c.maxIdle = max
		return nil
	}
}

// WithMaxItemSize rejects larger outgoing and incoming values. Zero disables
// the limit. The default is 1 MiB, matching a default memcached server.
func WithMaxItemSize(max int) Option {
	return func(c *config) error {
		if max < 0 {
			return fmt.Errorf("memcache: max item size must not be negative")
		}
		c.maxItemSize = max
		return nil
	}
}

// WithCodec sets the codec used by GetInto and SetValue.
func WithCodec(codec Codec) Option {
	return func(c *config) error {
		if codec == nil {
			return fmt.Errorf("memcache: nil codec")
		}
		c.codec = codec
		return nil
	}
}
