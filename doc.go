// Package memcache implements a modern memcached client using only the meta
// text protocol (mg, ms, md, ma, me, and mn).
//
// The Client's methods are the scenario layer: one verb per user scenario
// (Get, Fetch, Update, Incr, Drain, ...), returning business values. A miss
// is a normal answer, never an error; concurrency coordination (leases,
// compare-and-swap loops, request merging) is the library's job and never
// appears in caller code; failure behavior is an explicit policy (Degrade,
// OnError). Values are []byte in this release; the typed layer arrives with
// generic methods in go1.27.
//
// The 1:1 protocol layer remains fully available behind Client.Meta.
// Client is safe for concurrent use.
package memcache
