// Package memcache implements a modern memcached client using only the meta
// text protocol (mg, ms, md, ma, me, and mn).
//
// Client is safe for concurrent use. Values are bytes at the core API; Codec,
// GetAs, and SetValue provide an optional typed layer without hiding protocol
// metadata or mutation outcomes.
package memcache
