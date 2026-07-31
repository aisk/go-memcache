# memcache

`memcache` is a concurrent Go client for memcached's modern meta text protocol. It implements `mg`, `ms`, `md`, `ma`, `me`, and `mn`; it does not carry a legacy get/set protocol implementation.

The core API stores bytes and exposes CAS, TTL, stale/lease state, conditional operations, pipelining, and multi-server routing without turning normal cache outcomes into transport errors.

```go
client, err := memcache.New("127.0.0.1:11211")
if err != nil { /* handle */ }
defer client.Close()

ctx := context.Background()
if err := client.Set(ctx, "greeting", []byte("hello"), memcache.ExpiresIn(time.Minute)); err != nil {
    /* handle */
}
value, err := client.Get(ctx, "greeting")
if errors.Is(err, memcache.ErrCacheMiss) {
    // populate the cache
}
```

For protocol-aware operations, use `GetWithOptions`, `Store`, `DeleteWithOptions`, and `Arithmetic`. `ExecuteMeta` is the raw escape hatch. Keys containing whitespace or control bytes are automatically base64 encoded with the meta `b` flag.

## Typed values

`SetValue` and `GetInto` use the configured `Codec` (`JSONCodec` by default). Go does not yet allow a method to introduce its own type parameters, so the package-level `GetAs` is the temporary typed-get API:

```go
type Profile struct { Name string `json:"name"` }

_ = client.SetValue(ctx, "profile:1", Profile{Name: "Aki"}, memcache.ExpiresIn(time.Minute))
profile, err := memcache.GetAs[Profile](ctx, client, "profile:1")
```

## Batch and failure semantics

`Batch` validates every operation before writing, groups operations by server, and uses quiet commands with opaque IDs followed by an `mn` barrier. Results remain in input order, including duplicate keys. A backend failure is recorded on only that backend's results.

The client never automatically retries a command after writing begins. If a side-effect may have reached memcached but the response/barrier was not seen, the error is an `AmbiguousWriteError`; blindly retrying arithmetic, append, or prepend could otherwise apply the mutation twice.

Multiple servers use stable rendezvous hashing by default. `WithRouter` can replace it. Each server has an elastic concurrent connection pool: `maxIdle` limits retained idle connections, not active requests.

```go
client, err := memcache.NewServers([]string{
    "cache-a:11211",
    "cache-b:11211",
})
```

## License

MIT
