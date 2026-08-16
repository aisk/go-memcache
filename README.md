# memcache

English | [简体中文](README.zh-CN.md)

`memcache` is a concurrent Go client for memcached's modern meta text protocol (`mg`, `ms`, `md`, `ma`, `me`, `mn`). It does not carry a legacy get/set protocol implementation.

The API is designed to hide the wire protocol behind verbs named after what you are doing. CAS tokens never appear in caller code. Instead of reading a version and writing it back yourself, you call `Update` with a transform function and the client runs the read, compare and swap, retry loop internally. Instead of building lease or dogpile protection yourself, you call `Fetch` with a loader and the client coordinates so the value is computed once. When you do need the raw protocol, every meta command is still reachable through `Meta()`.

## Conventions

- Every method takes a `context.Context` as its first parameter.
- Values are `[]byte` and serialization stays with the caller. Empty values are rejected, because memcached represents lease placeholders as zero byte items.
- A miss is a normal answer, not an error. Reads return `(value, ok, err)` where `ok` reports presence and `err` reports infrastructure failure. The two never mix.
- Every method that stores a value takes its TTL as a positional `ttl time.Duration` parameter. There is no client wide default TTL. Passing `0` stores without expiration, and the constant `memcache.Forever` spells that choice out at the call site. A negative TTL is an error.

  ```go
  err = mc.Set(ctx, "config:site", buf, memcache.Forever)
  ```

- On verbs that auto create the key (`Incr`, `Decr`, `Append`, `Prepend`) the TTL applies only when the call creates the key. It never extends an existing key's lifetime.
- Optional modifiers are typed per verb. `Touch` is accepted only by `Get` and `GetMany`, `RefreshAhead` only by `Fetch`, so putting an option on a verb it has no meaning for is a compile error rather than a runtime surprise.

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

With multiple servers, keys are distributed by stable rendezvous hashing; `WithRouter` replaces the routing. Each server has an elastic connection pool. `WithMaxIdleConns` limits retained idle connections (not active requests) and idle connections are redialed after `WithIdleTimeout` (90 seconds by default).

Other options: `WithTimeout` (per request), `WithDialTimeout`, `WithNetwork`, `WithDialer`, `WithMaxItemSize`, plus the policy options `Degrade`, `OnError`, and a client wide `RefreshAhead` default described below.

## Reading

```go
func (c *Client) Get(ctx context.Context, key string, options ...GetOption) (value []byte, ok bool, err error)
func (c *Client) GetMany(ctx context.Context, keys []string, options ...GetOption) (map[string][]byte, error)
func (c *Client) Inspect(ctx context.Context, key string) (info ItemInfo, ok bool, err error)
```

`Get` reads one value.

```go
raw, ok, err := mc.Get(ctx, "user:42")
if err != nil { /* infrastructure failure */ }
if !ok { /* miss */ }
```

`GetMany` reads a set of keys in one round trip per backend and returns the hits. A miss is expressed by key absence in the returned map.

The `Touch(ttl)` option makes the same protocol command also slide each hit's expiration to `ttl`, which turns `Get` into the read half of session renewal.

```go
session, ok, err := mc.Get(ctx, "session:"+sid, memcache.Touch(30*time.Minute))
```

The slide is memcached's native touch and is blind: it extends whatever the read hits, including a value kept stale by `Invalidate`. A revocation that must stick goes through `Delete`.

`Inspect` returns an item's metadata (remaining TTL, size, last access, whether it was ever hit) without transferring the value or bumping its LRU position. It is an observability tool.

## Writing

```go
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
func (c *Client) SetMany(ctx context.Context, mapping map[string][]byte, ttl time.Duration) error
func (c *Client) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (ok bool, err error)
func (c *Client) Replace(ctx context.Context, key string, value []byte, ttl time.Duration) (ok bool, err error)
func (c *Client) Touch(ctx context.Context, key string, ttl time.Duration) error
func (c *Client) Delete(ctx context.Context, key string) error
func (c *Client) DeleteMany(ctx context.Context, keys []string) error
func (c *Client) Invalidate(ctx context.Context, key string, grace time.Duration) error
```

`Set` unconditionally stores a value for `ttl`. `SetMany` stores a batch in one round trip per backend, all sharing the same `ttl`.

`Add` stores only when the key is absent and reports whether this caller won, which makes it a simple distributed lock or once only guard.

```go
won, err := mc.Add(ctx, "job:daily-report", []byte("1"), 10*time.Minute)
if won { /* this process runs the job */ }
```

`Replace` stores only when the key still exists and reports whether it did. It is the write half of session renewal, where `false` means the session already ended.

`Touch` extends a key's TTL without transferring its value, as one blind protocol command. A missing key is not an error.

`Delete` removes a key. Deleting an absent key is a success, since the goal state already holds. `DeleteMany` removes a batch in one round trip per backend.

`Invalidate` marks a value stale instead of dropping it. For the `grace` period readers keep serving the old copy while `Fetch` elects one caller to recompute in the background; afterwards the key decays into a normal miss. `Invalidate` pairs with keys managed by `Fetch`: the `grace` bound holds only while nothing renews the key, since a touch slides it like any other expiration. Use `Delete` when the old value must not be served for even a second.

## Get or compute

```go
func (c *Client) Fetch(ctx context.Context, key string, ttl time.Duration,
    loader func(context.Context) ([]byte, error), options ...FetchOption) ([]byte, error)
```

`Fetch` is the highest frequency cache scenario as one verb. It returns the cached value, or runs `loader` to compute it and stores the result for `ttl`.

```go
report, err := mc.Fetch(ctx, "report:q3", time.Hour, buildReport)
```

On a miss, one caller across all processes wins a server side lease and runs the loader. Other goroutines in the same process wait on that result, and other processes wait briefly then compute locally without writing back. So the value is computed once, not once per waiter.

With the `RefreshAhead(window)` option, a value whose remaining TTL has entered the window is served immediately while one elected caller recomputes in a background goroutine, so no request ever pays the recompute latency.

```go
feed, err := mc.Fetch(ctx, "home:"+uid, 5*time.Minute, buildFeed,
    memcache.RefreshAhead(30*time.Second),
)
```

The loader runs on a context owned by the client, not the calling request's context, because its result may be shared by other waiters or outlive the caller entirely. All write backs are conditional on the version observed at election, so a key deleted mid recompute is never resurrected.

## Atomic modification

```go
func (c *Client) Update(ctx context.Context, key string, ttl time.Duration,
    fn func(current []byte, found bool) ([]byte, error)) ([]byte, error)
```

`Update` atomically transforms a value. It reads the current value with its version, applies `fn`, writes back only if nothing changed in between, and retries on conflict. On a miss `fn` receives `(nil, false)`. Returning an error from `fn` aborts without writing. `fn` may run multiple times, so it must be pure. If the retry loop keeps losing to concurrent writers, `Update` returns `ErrConflict`.

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

```go
func (c *Client) Incr(ctx context.Context, key string, delta uint64, ttl time.Duration) (uint64, error)
func (c *Client) Decr(ctx context.Context, key string, delta uint64, ttl time.Duration) (uint64, error)
```

`Incr` adds `delta` to a decimal counter and returns the new value, creating the counter on a miss so the first request counts as `delta`. `Decr` subtracts and saturates at zero. Since the `ttl` is fixed at creation and later calls never extend it, this is exactly fixed window rate limiting.

```go
n, err := mc.Incr(ctx, "rate:"+ip, 1, time.Minute)
```

```go
func (c *Client) Append(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Prepend(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Take(ctx context.Context, key string) ([]byte, error)
```

`Append` and `Prepend` concatenate raw bytes onto a value, creating it on a miss. `Take` atomically reads a value and deletes it, with no window in which concurrently appended bytes can be lost. Together they make a simple collect then drain pattern, such as buffering events and periodically taking the batch. A `nil` result from `Take` means there was nothing to take.

## Failure policy

By default every infrastructure failure surfaces as an error. The `Degrade(true)` client option makes reads report failures as misses and unconditional writes give up silently, because a cache outage should not become a site outage. Every absorbed error still reaches the `OnError` hook.

```go
mc, err := memcache.NewServers(servers,
    memcache.Degrade(true),
    memcache.OnError(func(err error) { log.Print(err) }),
)
```

Verbs whose answer feeds a business decision (`Add`, `Replace`, `Update`, `Incr`, `Decr`, `Take`) keep failing loudly even under `Degrade`, and an `AmbiguousWriteError` (the write may have landed) always surfaces. The client never automatically retries a command after writing begins, since blindly retrying arithmetic or append could apply the mutation twice.

## Protocol access

Everything not covered by a scenario verb lives behind `Meta()`, a 1:1 mapping of the meta protocol that returns typed results instead of collapsing protocol states into errors.

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
raw, err := mc.Meta().Execute(ctx, memcache.MetaCommand{Command: "mg", Key: key, Flags: []string{"v", "t"}})
```

`Batch` validates every operation before writing, groups operations by server, and pipelines them with quiet commands. Results stay in input order and a backend failure is recorded on only that backend's results. Keys containing whitespace or control bytes are automatically base64 encoded with the meta `b` flag.

## License

MIT
