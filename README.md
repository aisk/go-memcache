# memcache

English | [简体中文](README.zh-CN.md)

`memcache` is a concurrent Go client for memcached, speaking the modern meta text protocol. It does not carry a legacy get/set protocol implementation.

The API hides the wire protocol behind verbs named for what you are doing. The meta protocol's CAS tokens and leases never surface in caller code. Instead of reading a version and writing it back, call `Update` with a transform function and the client runs the read, compare and swap, retry loop internally. Instead of building dogpile protection, call `Fetch` with a loader and the client makes sure the value is computed once. When you do need the raw protocol, every meta command is still reachable through `Meta()`.

## Creating a client

```go
func New(server string, options ...Option) (*Client, error)
func NewServers(servers []string, options ...Option) (*Client, error)
func (c *Client) Close() error
```

```go
mc, err := memcache.New("127.0.0.1:11211")
if err != nil { /* handle */ }
defer mc.Close()
```

With multiple servers, keys are distributed by stable rendezvous hashing and `WithRouter` replaces the routing. Each server has an elastic connection pool. `WithMaxIdleConns` limits retained idle connections (not active requests) and idle connections are redialed after `WithIdleTimeout` (90 seconds by default).

Other options are `WithTimeout` (per request), `WithDialTimeout`, `WithNetwork`, `WithDialer`, `WithMaxItemSize`, plus the policy options `WithCodec`, `Degrade`, `OnError` and a client wide `RefreshAhead` default described below.

Every method takes a `context.Context` as its first parameter.

## Values and keys

The object verbs (`Get`, `GetMany`, `Set`, `SetMany`, `Add`, `Replace`, `Fetch`, `Update`) are generic methods. Their value type `T` is inferred from the argument or callback wherever one exists, and is spelled out only on `Get` and `GetMany`, where the value appears in the return position alone. Values are encoded with the client's `Codec`, `JSONCodec` by default, and `WithCodec` installs another.

```go
type Codec interface {
    Marshal(value any) ([]byte, error)
    Unmarshal(data []byte, value any) error
}
```

`[]byte` is the identity type: it is stored and read back untouched, whatever the codec, so `Get[[]byte]` always returns the raw stored bytes. The counter verbs (`Incr`, `Decr`) and the raw bytes verbs (`Append`, `Prepend`, `Take`) never consult the codec. A value that fails to encode or decode is an error, never a miss.

Empty encodings are rejected, because memcached represents lease placeholders as zero byte items.

Any string is a usable key. Keys containing whitespace or control bytes are automatically base64 encoded on the wire with the meta `b` flag.

## Reading

```go
func (c *Client) Get[T any](ctx context.Context, key string, options ...GetOption) (value T, ok bool, err error)
func (c *Client) GetMany[T any](ctx context.Context, keys []string, options ...GetOption) (map[string]T, error)
func (c *Client) Inspect(ctx context.Context, key string) (info ItemInfo, ok bool, err error)
```

`Get` reads one value. A miss is a normal answer, not an error. Reads return `(value, ok, err)` where `ok` reports presence and `err` reports infrastructure failure. The two never mix.

```go
user, ok, err := mc.Get[User](ctx, "user:"+uid)
if err != nil { /* infrastructure failure or undecodable value */ }
if !ok {
    user = loadUser(uid)
    mc.Set(ctx, "user:"+uid, user, 10*time.Minute)
}
```

`GetMany` reads a set of keys in one round trip per backend and returns the hits. A miss is expressed by key absence in the returned map.

The `Touch(ttl)` option makes the same protocol command also slide each hit's expiration to `ttl`, which turns `Get` into the read half of session renewal. Options are typed per verb. `Touch` is accepted only by `Get` and `GetMany`, `RefreshAhead` only by `Fetch`, so putting an option on a verb it has no meaning for is a compile error rather than a runtime surprise.

```go
session, ok, err := mc.Get[Session](ctx, "session:"+sid, memcache.Touch(30*time.Minute))
```

The slide is memcached's native touch and is blind. It extends whatever the read hits, including a value kept stale by `Invalidate`. A revocation that must stick goes through `Delete`.

`Inspect` returns an item's metadata (remaining TTL, size, last access, whether it was ever hit) without transferring the value or bumping its LRU position. It is an observability tool for debugging. Branching business logic on metadata is inherently racy and not a supported pattern.

## Writing

```go
func (c *Client) Set[T any](ctx context.Context, key string, value T, ttl time.Duration) error
func (c *Client) SetMany[T any](ctx context.Context, mapping map[string]T, ttl time.Duration) error
func (c *Client) Add[T any](ctx context.Context, key string, value T, ttl time.Duration) (ok bool, err error)
func (c *Client) Replace[T any](ctx context.Context, key string, value T, ttl time.Duration) (ok bool, err error)
func (c *Client) Touch(ctx context.Context, key string, ttl time.Duration) error
func (c *Client) Delete(ctx context.Context, key string) error
func (c *Client) DeleteMany(ctx context.Context, keys []string) error
func (c *Client) Invalidate(ctx context.Context, key string, grace time.Duration) error
```

`Set` unconditionally stores a value for `ttl`. `SetMany` stores a batch in one round trip per backend, all sharing the same `ttl`.

Every storing method takes its TTL as a positional `ttl time.Duration` parameter, with no client wide default. Passing `0` stores without expiration, and the constant `memcache.Forever` spells that choice out at the call site. A negative TTL is an error.

```go
err = mc.Set(ctx, "config:site", siteConfig, memcache.Forever)
```

`Add` stores only when the key is absent and reports whether this caller won, which makes it a simple once only guard for multi instance deployments.

```go
won, err := mc.Add(ctx, "job:daily-report:"+today, []byte("1"), 24*time.Hour)
if won { /* this process runs the job */ }
```

`Replace` stores only when the key still exists and reports whether it did. It is the write half of session renewal. If the user logged out mid request, an unconditional `Set` would resurrect the dead session, `Replace` will not.

```go
ok, err := mc.Replace(ctx, "session:"+sid, session, 30*time.Minute)
```

`Touch` extends a key's TTL without transferring its value, as one blind protocol command. It exists for large values (rendered pages, serialized reports) where reading the payload back just to renew it wastes bandwidth. When you are reading anyway, use `Get` with the `Touch` option. A missing key is not an error.

`Delete` removes a key outright and the next reader pays a full miss. Deleting an absent key is a success, since the goal state already holds. `DeleteMany` removes a batch in one round trip per backend.

`Invalidate` marks the value stale instead of dropping it.

```go
mc.Delete(ctx, "article:"+aid)                    // hard, old data must not reappear
mc.Invalidate(ctx, "article:"+aid, time.Minute)   // soft, readers keep the old copy briefly
```

During the grace period plain readers keep getting the old copy while `Fetch` elects one caller to recompute in the background, and afterwards the key decays into a normal miss. `Invalidate` pairs with keys managed by `Fetch`. The `grace` bound holds only while nothing renews the key, since a touch slides it like any other expiration. Use `Delete` when the old value must not be served for even a second.

## Get or compute

```go
func (c *Client) Fetch[T any](ctx context.Context, key string, ttl time.Duration,
    loader func(context.Context) (T, error), options ...FetchOption) (T, error)
```

`Fetch` is the highest frequency cache pattern as one verb. It returns the cached value, or runs `loader` to compute it and stores the result for `ttl`.

```go
report, err := mc.Fetch(ctx, "report:q3", time.Hour, buildReport)
```

On a miss, one caller across all processes wins a server side lease and runs the loader. Other goroutines in the same process wait on that result, and other processes wait briefly then compute locally without writing back. So a hot key expiring under a thousand concurrent requests costs one recomputation, not a thousand.

With the `RefreshAhead(window)` option, a value whose remaining TTL has entered the window is served immediately while one elected caller recomputes in a background goroutine, so no request ever pays the recompute latency and the curve never shows an expiry spike.

```go
feed, err := mc.Fetch(ctx, "home:"+uid, 5*time.Minute, buildFeed,
    memcache.RefreshAhead(30*time.Second),
)
```

The loader runs on a context owned by the client, not the calling request's context, because its result may be shared by other waiters or outlive the caller entirely. Every caller, the winner included, receives its own decoded copy of the stored form, so what `Fetch` returns is always what a later `Get` would return. Every write back is conditional on the version observed at election, so a key deleted mid recompute is never resurrected. Write back failures never change what `Fetch` returns, they go to the `OnError` hook. `Fetch` never fails because coordination failed. Every path ends in a value, the loader's own error, or the caller's context error.

## Atomic modification

```go
func (c *Client) Update[T any](ctx context.Context, key string, ttl time.Duration,
    fn func(current T, found bool) (T, error)) (T, error)
```

`Update` atomically transforms a value. It reads the current value with its version, applies `fn`, writes back only if nothing changed in between, and retries on conflict. On a miss `fn` receives the zero value of `T` and `false`. Returning an error from `fn` aborts the whole operation, the entry is left unwritten and the error propagates unchanged. `fn` may run multiple times, so it must be pure. If the retry loop keeps losing to concurrent writers, `Update` returns `ErrConflict`. A value kept stale by `Invalidate` counts as a miss, because transforming invalidated data would silently launder it back to fresh.

```go
cart, err := mc.Update(ctx, "cart:"+uid, 30*time.Minute,
    func(items []Item, found bool) ([]Item, error) {
        return append(items, item), nil
    },
)
```

```go
func (c *Client) Incr(ctx context.Context, key string, delta uint64, ttl time.Duration) (uint64, error)
func (c *Client) Decr(ctx context.Context, key string, delta uint64, ttl time.Duration) (uint64, error)
```

`Incr` adds `delta` to a decimal counter and returns the new value, creating the counter on a miss so the first request counts as `delta`. `Decr` subtracts and saturates at zero. The `ttl` applies only when the call creates the counter and later calls never extend it, which is exactly fixed window rate limiting.

```go
n, err := mc.Incr(ctx, "rate:"+ip, 1, time.Minute)
if n > 100 { /* reject the request */ }
```

```go
func (c *Client) Append(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Prepend(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Take(ctx context.Context, key string) ([]byte, error)
```

`Append` and `Prepend` concatenate raw bytes onto a value, creating it on a miss. The `ttl` applies only to that creation, so later calls never extend the buffer's life. `Take` atomically reads a value and deletes it, with no window in which concurrently appended bytes can be lost. Together they make a collect then drain pattern, such as buffering events per user and periodically taking the batch.

```go
err = mc.Append(ctx, "events:"+uid, []byte("login;"), 24*time.Hour)
buffered, err := mc.Take(ctx, "events:"+uid)   // bytes, split by the caller
```

A `nil` result from `Take` means there was nothing to take. `Take` is not limited to byte streams, taking a one time token stored with `Set` works the same way.

## Failure policy

By default every infrastructure failure surfaces as an error. The `Degrade(true)` client option decouples a cache outage from a site outage.

```go
mc, err := memcache.NewServers(servers,
    memcache.Degrade(true),
    memcache.OnError(func(err error) { log.Print(err) }),
)
```

Under `Degrade`, reads report failures as misses, `Fetch` computes locally without writing back, and blind writes (`Set`, `Delete`, `Touch`, `Invalidate`, `Append`, ...) give up silently. Verbs whose answer feeds a business decision (`Add`, `Replace`, `Update`, `Incr`, `Decr`, `Take`) keep failing loudly even under `Degrade`, because inventing an answer is worse than failing. An `AmbiguousWriteError` (the write may have landed) always surfaces. Degrading covers "the cache is down", never "the write may or may not have happened". Every absorbed failure still reaches the `OnError` hook, so degrading business behavior never degrades observability. The client never automatically retries a command after writing begins, since blindly retrying arithmetic or append could apply the mutation twice.

## Protocol access

Everything the Client's verbs do not cover lives behind `Meta()`, a 1:1 mapping of the meta protocol (`mg`/`ms`/`md`/`ma`/`me`) that returns typed results instead of collapsing protocol states into errors.

```go
func (m *MetaClient) Get(ctx context.Context, key string, options MetaGetOptions) (GetResult, error)
func (m *MetaClient) Set(ctx context.Context, key string, value []byte, options MetaSetOptions) (MutationResult, error)
func (m *MetaClient) Delete(ctx context.Context, key string, options MetaDeleteOptions) (MutationResult, error)
func (m *MetaClient) Arithmetic(ctx context.Context, key string, options MetaArithmeticOptions) (ArithmeticResult, error)
func (m *MetaClient) Execute(ctx context.Context, command MetaCommand) (RawResponse, error)
func (m *MetaClient) Batch(ctx context.Context, operations []Operation) ([]OperationResult, error)
func (m *MetaClient) Debug(ctx context.Context, key string) (map[string]string, error)
func (m *MetaClient) Noop(ctx context.Context) error
```

```go
result, err := mc.Meta().Get(ctx, key, memcache.MetaGetOptions{ReturnCAS: true, ReturnTTL: true})

// Framing-safe bytes-level escape hatch for anything not covered above.
raw, err := mc.Meta().Execute(ctx, memcache.MetaCommand{Command: "mg", Key: key, Flags: []string{"v", "t"}})
```

`Batch` validates every operation before writing, groups operations by server, and pipelines them with quiet commands. Results stay in input order and a backend failure is recorded on only that backend's results.

## License

MIT
