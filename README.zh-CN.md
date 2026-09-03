# memcache

[English](README.md) | 简体中文

`memcache` 是一个使用 memcached 现代 meta 文本协议的并发 Go 客户端，不包含旧版 get/set 协议的实现。

API 的设计意图是把底层协议屏蔽在以使用场景命名的方法后面，调用方无需了解 meta 协议里的 CAS 和 lease。不必自己读出版本号再写回去，调用 `Update` 传入一个变换函数，客户端在内部完成读取、比较交换、重试的循环。也不必自己实现防击穿，调用 `Fetch` 传入一个 loader，客户端保证值只被计算一次。确实需要原始协议时，所有 meta 命令仍然可以通过 `Meta()` 访问。

## 创建客户端

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

多服务器时，key 通过稳定的 rendezvous 哈希分布，`WithRouter` 可以替换路由策略。每个服务器一条多路复用连接。并发发出的命令在这条连接上排队，一次 flush 写出，按顺序收到应答，所以 N 个 goroutine 同时访问一台服务器只付一轮往返、只占一个 socket。`WithMaxConns` 允许每台服务器开更多连接，只有当现有连接全都有命令在途时才会再拨一条。没有命令在途的连接空闲超过 `WithIdleTimeout`（默认 90 秒）后关闭，下次需要时重新拨号。

其他选项有 `WithTimeout`（单次请求）、`WithDialTimeout`、`WithNetwork`、`WithDialer`、`WithMaxItemSize`，以及下文介绍的策略选项 `WithCodec`、`Degrade`、`OnError` 和客户端级的 `RefreshAhead` 默认值。

每个方法的第一个参数都是 `context.Context`。

## 值与 key

对象类动词（`Get`、`GetMany`、`Set`、`SetMany`、`Add`、`Replace`、`Fetch`、`FetchMany`、`Update`）是泛型方法。值类型 `T` 只要出现在实参或回调里就由编译器推导，只有 `Get` 和 `GetMany` 需要写出来，因为值只出现在返回值中。值用客户端的 `Codec` 编解码，默认是 `JSONCodec`，`WithCodec` 可以换成别的。

```go
type Codec interface {
    Marshal(value any) ([]byte, error)
    Unmarshal(data []byte, value any) error
}
```

`[]byte` 是恒等类型：不论用什么 codec，它都原样存入、原样读出，所以 `Get[[]byte]` 总是拿到原始字节。计数器动词（`Incr`、`Decr`）和原始字节动词（`Append`、`Prepend`、`Take`）从不经过 codec。编码或解码失败是错误，不会伪装成未命中。

空的编码结果会被拒绝，因为 memcached 用零字节条目表示 lease 占位符。

任何字符串都可以作为 key。包含空白或控制字节的 key 会在协议层自动用 meta 的 `b` 标志做 base64 编码。

## 读取

```go
func (c *Client) Get[T any](ctx context.Context, key string, options ...GetOption) (value T, ok bool, err error)
func (c *Client) GetMany[T any](ctx context.Context, keys []string, options ...GetOption) (map[string]T, error)
func (c *Client) Inspect(ctx context.Context, key string) (info ItemInfo, ok bool, err error)
```

`Get` 读取一个值。未命中是正常的回答，不是错误。读操作返回 `(value, ok, err)`，`ok` 表示是否存在，`err` 表示基础设施故障，两者永不混淆。

```go
user, ok, err := mc.Get[User](ctx, "user:"+uid)
if err != nil { /* infrastructure failure or undecodable value */ }
if !ok {
    user = loadUser(uid)
    mc.Set(ctx, "user:"+uid, user, 10*time.Minute)
}
```

`GetMany` 对每个后端一次往返批量读取一组 key，返回命中的部分，未命中表现为返回 map 中 key 的缺失。

`Touch(ttl)` 选项让同一条协议命令在读取的同时把命中值的过期时间顺延到 `ttl`，这使 `Get` 成为会话续期的读取一半。可选修饰符按方法类型化，`Touch` 只被 `Get` 和 `GetMany` 接受，`RefreshAhead` 只被 `Fetch` 接受，把选项用在没有意义的方法上是编译错误，而不是运行时的意外。

```go
session, ok, err := mc.Get[Session](ctx, "session:"+sid, memcache.Touch(30*time.Minute))
```

这个顺延就是 memcached 原生的 touch，是盲目的：读到什么就延长什么，包括被 `Invalidate` 标记为过时的值。必须立刻生效的撤销要走 `Delete`。

`Inspect` 返回条目的元数据（剩余 TTL、大小、最近访问时间、是否曾被命中），不传输值也不影响它的 LRU 位置。它是一个调试用的观测工具，用元数据做业务分支本质上有竞态，不是受支持的用法。

## 写入

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

`Set` 无条件存储一个值，有效期为 `ttl`。`SetMany` 对每个后端一次往返批量存储，所有值共享同一个 `ttl`。

每个写入值的方法都把 TTL 作为位置参数 `ttl time.Duration` 接收，客户端没有全局默认 TTL。传 `0` 表示永不过期，也可以用常量 `memcache.Forever` 让这个选择在调用处一目了然。负的 TTL 是错误。

```go
err = mc.Set(ctx, "config:site", siteConfig, memcache.Forever)
```

`Add` 只在 key 不存在时存储，并报告本次调用是否成功，可以直接当作多实例部署下只执行一次的保护。

```go
won, err := mc.Add(ctx, "job:daily-report:"+today, []byte("1"), 24*time.Hour)
if won { /* this process runs the job */ }
```

`Replace` 只在 key 仍然存在时存储，并报告是否写入了。它是会话续期的写入一半。如果用户在请求中途登出，无条件的 `Set` 会复活已死的会话，`Replace` 不会。

```go
ok, err := mc.Replace(ctx, "session:"+sid, session, 30*time.Minute)
```

`Touch` 只延长 key 的 TTL，不传输值，是一条盲目的协议命令。它为大值（渲染好的页面、序列化的报表）而存在，只为续期把整个负载读回来是浪费带宽。本来就要读取时，用带 `Touch` 选项的 `Get`。key 不存在不算错误。

`Delete` 直接删除一个 key，下一个读者承担一次完整的未命中。删除不存在的 key 视为成功，因为目标状态已经成立。`DeleteMany` 对每个后端一次往返批量删除。

`Invalidate` 把值标记为过时而不是直接删除。

```go
mc.Delete(ctx, "article:"+aid)                    // 硬失效，旧数据不能再出现
mc.Invalidate(ctx, "article:"+aid, time.Minute)   // 软失效，读者短暂拿到旧值
```

在 `grace` 期间普通读者继续拿到旧值，同时 `Fetch` 会选出一个调用者在后台重新计算，之后这个 key 衰变为正常的未命中。`Invalidate` 与 `Fetch` 管理的 key 配套使用。`grace` 只在没有人续期这个 key 时才是上界，touch 会像顺延普通过期时间一样顺延它。如果旧值一秒都不能再被返回，就用 `Delete`。

## 读取或计算

```go
func (c *Client) Fetch[T any](ctx context.Context, key string, ttl time.Duration,
    loader func(context.Context) (T, error), options ...FetchOption) (T, error)
```

`Fetch` 把最高频的缓存场景做成一个方法。它返回缓存中的值，或者运行 `loader` 计算它并以 `ttl` 存储结果。

```go
report, err := mc.Fetch(ctx, "report:q3", time.Hour, buildReport)
```

未命中时，所有进程中只有一个调用者赢得服务端 lease 并运行 loader。同进程内的其他 goroutine 等待这个结果，其他进程短暂等待后本地计算但不写回。因此一个热点 key 在上千个并发请求下过期，代价是一次重算，而不是一千次。

加上 `RefreshAhead(window)` 选项后，剩余 TTL 进入窗口的值会被立即返回，同时选出一个调用者在后台 goroutine 中重新计算，没有任何请求需要承担重算的延迟，曲线上也不会出现过期尖刺。

```go
feed, err := mc.Fetch(ctx, "home:"+uid, 5*time.Minute, buildFeed,
    memcache.RefreshAhead(30*time.Second),
)
```

loader 运行在客户端持有的 context 上而不是调用请求的 context 上，因为它的结果可能被其他等待者共享，也可能比调用者活得更久。每个调用者（包括赢得选举的那个）拿到的都是存储形态解码出的独立副本，所以 `Fetch` 返回的永远和之后 `Get` 读到的一致。所有写回都以选举时观察到的版本为条件，因此重算过程中被删除的 key 永远不会被复活。写回失败不会改变 `Fetch` 的返回值，它们进入 `OnError` 钩子。`Fetch` 不会因为协调失败而失败，每条路径的终点都是一个值、loader 自己的错误，或调用者的 context 错误。

```go
func (c *Client) FetchMany[T any](ctx context.Context, keys []string, ttl time.Duration,
    loader func(ctx context.Context, missing []string) (map[string]T, error), options ...FetchOption) (map[string]T, error)
```

`FetchMany` 是一组 key 上的 `Fetch`。它对每个后端一次往返读出全部 key，返回缓存中已有的值，其余的 key 一次性交给 `loader`，结果以 `ttl` 存储。一次渲染几十个对象的页面在它们同时过期时只付一次读取和一次回源，也不会有请求重算别人正在算的东西。

```go
users, err := mc.FetchMany(ctx, userKeys, time.Hour, func(ctx context.Context, missing []string) (map[string]User, error) {
    return db.LoadUsers(ctx, missing)
})
```

每个 key 的协调方式和 `Fetch` 对单个 key 完全一样，逐 key 成立：未命中时跨进程只有一个调用者赢得 lease，同进程的其他 goroutine 等待它的结果，正在被加载的 key 上并发的 `Fetch` 会直接加入。`RefreshAhead` 同样有效，这次读取赢得刷新 lease 的所有 key 由一次后台 loader 调用重算。loader 没有返回的 key 不进结果，就像在数据源未命中一样，它的 lease 会被释放让下一次调用重新选举。loader 出错则整个调用失败。

## 原子修改

```go
func (c *Client) Update[T any](ctx context.Context, key string, ttl time.Duration,
    fn func(current T, found bool) (T, error)) (T, error)
```

`Update` 原子地变换一个值。它带版本读出当前值，应用 `fn`，只在中间没有别人改动时写回，冲突则重试。未命中时 `fn` 收到 `T` 的零值和 `false`。`fn` 返回错误会中止整个操作，条目保持未写入，错误原样传出。`fn` 可能运行多次，因此必须是纯函数。如果重试循环一直输给并发写入者，`Update` 返回 `ErrConflict`。被 `Invalidate` 标记为过时的值按未命中处理，因为变换已失效的数据等于悄悄把它洗回新鲜。

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

`Incr` 给十进制计数器加上 `delta` 并返回新值，未命中时创建计数器，所以第一次请求计为 `delta`。`Decr` 做减法并在零处饱和。`ttl` 只在这次调用创建了计数器时生效，之后的调用不会延长它，这正是固定窗口限流需要的行为。

```go
n, err := mc.Incr(ctx, "rate:"+ip, 1, time.Minute)
if n > 100 { /* reject the request */ }
```

```go
func (c *Client) Append(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Prepend(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Take(ctx context.Context, key string) ([]byte, error)
```

`Append` 和 `Prepend` 把原始字节拼接到值的尾部或头部，未命中时创建值。`ttl` 只作用于那次创建，之后的调用不会延长这个缓冲区的寿命。`Take` 原子地读出一个值并删除它，不存在并发追加的字节丢失的窗口。两者合起来构成收集再取走的模式，比如按用户缓冲事件并定期取走一批。

```go
err = mc.Append(ctx, "events:"+uid, []byte("login;"), 24*time.Hour)
buffered, err := mc.Take(ctx, "events:"+uid)   // 字节流，由调用方切分
```

`Take` 返回 `nil` 表示没有东西可取。`Take` 不限于字节流，取走一个用 `Set` 存储的一次性令牌也是同样的用法。

## 故障策略

默认情况下每个基础设施故障都以错误形式返回。客户端选项 `Degrade(true)` 把缓存故障和整站故障解耦。

```go
mc, err := memcache.NewServers(servers,
    memcache.Degrade(true),
    memcache.OnError(func(err error) { log.Print(err) }),
)
```

在 `Degrade` 下，读操作把故障报告为未命中，`Fetch` 和 `FetchMany` 本地计算但不写回，盲目写入（`Set`、`Delete`、`Touch`、`Invalidate`、`Append` 等）静默放弃。结果用于业务判断的方法（`Add`、`Replace`、`Update`、`Incr`、`Decr`、`Take`）仍然报错，因为编造一个答案比失败更糟。`AmbiguousWriteError`（写入可能已经生效）也总是返回，降级覆盖的是"缓存挂了"，从来不是"写入可能生效也可能没有"。每个被吸收的错误仍然会到达 `OnError` 钩子，业务行为的降级不会降级可观测性。写入开始后客户端永远不会自动重试命令，因为盲目重试算术或追加可能让变更生效两次。

## 协议层

场景方法没有覆盖的一切都在 `Meta()` 后面，它是 meta 协议（`mg`/`ms`/`md`/`ma`/`me`）的 1:1 映射，返回类型化的结果而不是把协议状态折叠成错误。

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

`Batch` 在写出前校验每个操作，按服务器分组，用 quiet 命令流水线执行。结果保持输入顺序，某个后端的故障只记录在该后端的结果上。

## 许可证

MIT
