# memcache

English | [简体中文](README.zh-CN.md)

`memcache` is a concurrent Go client for memcached's modern meta text protocol. It implements `mg`, `ms`, `md`, `ma`, `me`, and `mn`; it does not carry a legacy get/set protocol implementation.

The client is scenario oriented: one verb per user scenario, built on four axioms. A miss is a normal answer, not an error. Every cached value has a source of truth and a recompute path, so "get or compute" is a first-class verb. Concurrency coordination (leases, compare-and-swap loops, request merging) is the library's job and never appears in caller code. The cache is an availability optimization, so failure behavior is an explicit policy.

Values are `[]byte` in this release; serialization stays with the caller until Go ships generic methods (go1.27), at which point the same verbs gain type parameters without changing shape.

## Reading and writing

```go
mc, err := memcache.New("127.0.0.1:11211")
if err != nil { /* handle */ }
defer mc.Close()

ctx := context.Background()
raw, ok, err := mc.Get(ctx, "user:42")   // value, presence, failure: three axes that never mix
if err != nil { /* infrastructure failure */ }
if !ok { /* miss: recompute and store */ }

err = mc.Set(ctx, "user:42", buf, 10*time.Minute)
```

Every operation that stores a value takes its TTL as a positional parameter, matching the go-redis convention. There is no client-wide default TTL: a call site always shows how long its data lives, and storing without expiration is the explicit choice `memcache.Forever`.

```go
err = mc.Set(ctx, "config:site", buf, memcache.Forever)
```

Optional modifiers are typed per verb: `RefreshAhead` is accepted only by `Fetch`; `Window` only by counters and streams. Putting an option on a verb it has no meaning for is a compile error, not a runtime surprise.

## Get or compute: Fetch

`Fetch` is the highest-frequency cache scenario as one verb: return the cached value, or compute it exactly once.

```go
report, err := mc.Fetch(ctx, "report:q3", time.Hour, buildReport)
```

On a miss, one caller across all processes wins a server-side lease (meta vivify) and runs the loader; other goroutines in the same process wait on that result, other processes wait briefly and then compute locally without writing back. With `RefreshAhead`, a value nearing expiry is served immediately while one elected caller recomputes in a background goroutine, so no request ever pays the recompute latency:

```go
feed, err := mc.Fetch(ctx, "home:"+uid, 5*time.Minute, buildFeed,
    memcache.RefreshAhead(30*time.Second),
)
```

`Invalidate(ctx, key, grace)` marks a value stale instead of deleting it: readers keep the old copy for the grace period while `Fetch` elects one caller to refresh in the background. `Delete` is the hard variant. All write-backs are conditional on the version observed at election, so a key deleted mid-recompute is never resurrected.

## Concurrent modification: Update and Drain

`Update` runs the read-transform-conditional-write-retry loop internally; version tokens never appear in user code:

```go
cart, err := mc.Update(ctx, "cart:"+uid, 30*time.Minute,
    func(current []byte, found bool) ([]byte, error) {
        var items []Item
        if found {
            if err := json.Unmarshal(current, &items); err != nil {
                return nil, err
            }
        }
        return json.Marshal(append(items, item))
    },
)
```

`Drain` atomically takes and clears a byte-stream buffer built with `Append`/`Prepend`, with no window in which concurrently appended events can be lost. `Incr`/`Decr` auto-create counters inside a fixed `Window`, which is exactly fixed-window rate limiting:

```go
n, err := mc.Incr(ctx, "rate:"+ip, 1, memcache.Window(time.Minute))
```

## Failure policy

By default every infrastructure failure surfaces as an error. `Degrade(true)` makes reads report failures as misses and unconditional writes give up silently, because a cache outage should not be a site outage; every absorbed error still reaches the `OnError` hook. Verbs whose answer feeds a business decision (`Add`, `Replace`, `Update`, `Incr`, `Decr`, `Drain`) keep failing loudly, and an `AmbiguousWriteError` (the write may have landed) always surfaces.

```go
mc, err := memcache.NewServers(servers,
    memcache.Degrade(true),
    memcache.OnError(func(err error) { log.Print(err) }),
)
```

The client never automatically retries a command after writing begins; blindly retrying arithmetic or append could apply the mutation twice.

## Protocol layer

Everything not covered by a scenario verb lives behind `Meta()`, a 1:1 mapping of the meta protocol that returns typed results instead of collapsing protocol states into errors:

```go
result, err := mc.Meta().Get(ctx, key, memcache.GetOptions{ReturnCAS: true, ReturnTTL: true})
raw, err := mc.Meta().Execute(ctx, memcache.MetaCommand{Command: "mg", Key: key, Flags: []string{"v", "t"}})
```

`Meta().Batch` validates every operation before writing, groups operations by server, and pipelines them with quiet commands behind an `mn` barrier; results stay in input order and a backend failure is recorded on only that backend's results. Keys containing whitespace or control bytes are automatically base64 encoded with the meta `b` flag.

Multiple servers use stable rendezvous hashing by default; `WithRouter` can replace it. Each server has an elastic concurrent connection pool: `WithMaxIdleConns` limits retained idle connections, not active requests. Idle connections are reused most-recently-released first and are redialed once idle longer than `WithIdleTimeout` (90 seconds by default).

```go
mc, err := memcache.NewServers([]string{"cache-a:11211", "cache-b:11211"})
```

## License

MIT
