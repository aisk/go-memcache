// Package memcache implements a modern memcached client using only the meta
// text protocol (mg, ms, md, ma, me, and mn).
//
// The Client's methods are one verb per operation (Get, Fetch, Update,
// Incr, Take, ...), each returning business values. A miss
// is a normal answer, never an error; concurrency coordination (leases,
// compare-and-swap loops, request merging) is the library's job and never
// appears in caller code; failure behavior is an explicit policy (Degrade,
// OnError). The object verbs are generic methods: values are encoded with
// the client's Codec (JSON by default), and []byte always passes through
// untouched.
//
// The 1:1 protocol layer remains fully available behind Client.Meta.
// Client is safe for concurrent use.
package memcache
