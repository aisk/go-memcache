# memcache

[English](README.md) | 简体中文

`memcache` 是一个面向 memcached 现代 meta 文本协议（`mg`、`ms`、`md`、`ma`、`me`、`mn`）的并发 Go 客户端，不包含旧版 get/set 协议的实现。

API 的设计意图是把底层协议屏蔽在以使用场景命名的方法后面。CAS 令牌不会出现在调用方代码里。你不需要自己读出版本号再写回去，而是调用 `Update` 传入一个变换函数，客户端在内部完成读取、比较交换、重试的循环。你也不需要自己实现 lease 或防击穿逻辑，而是调用 `Fetch` 传入一个 loader，客户端负责协调，保证值只被计算一次。当你确实需要原始协议时，所有 meta 命令仍然可以通过 `Meta()` 访问。

## 通用规则

- 每个方法的第一个参数都是 `context.Context`。
- 值的类型是 `[]byte`，序列化由调用方负责。空值会被拒绝，因为 memcached 用零字节条目表示 lease 占位符。
- 未命中是正常的回答，不是错误。读操作返回 `(value, ok, err)`，`ok` 表示是否存在，`err` 表示基础设施故障，两者永不混淆。
- 每个写入值的方法都把 TTL 作为位置参数 `ttl time.Duration` 接收，客户端没有全局默认 TTL。传 `0` 表示永不过期，也可以用常量 `memcache.Forever` 让这个选择在调用处一目了然。负的 TTL 是错误。

  ```go
  err = mc.Set(ctx, "config:site", buf, memcache.Forever)
  ```

- 在会自动创建 key 的方法上（`Incr`、`Decr`、`Append`、`Prepend`），TTL 只在这次调用创建了 key 时生效，永远不会延长已存在 key 的寿命。
- 可选修饰符按方法类型化。`Touch` 只被 `Get` 和 `GetMany` 接受，`RefreshAhead` 只被 `Fetch` 接受，把选项用在没有意义的方法上是编译错误，而不是运行时的意外。

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

多服务器时，key 通过稳定的 rendezvous 哈希分布，`WithRouter` 可以替换路由策略。每个服务器有一个弹性连接池，`WithMaxIdleConns` 限制保留的空闲连接数（不是活跃请求数），空闲超过 `WithIdleTimeout`（默认 90 秒）的连接会被重新拨号。

其他选项有 `WithTimeout`（单次请求）、`WithDialTimeout`、`WithNetwork`、`WithDialer`、`WithMaxItemSize`，以及下文介绍的策略选项 `Degrade`、`OnError` 和客户端级的 `RefreshAhead` 默认值。

## 读取

```go
func (c *Client) Get(ctx context.Context, key string, options ...GetOption) (value []byte, ok bool, err error)
func (c *Client) GetMany(ctx context.Context, keys []string, options ...GetOption) (map[string][]byte, error)
func (c *Client) Inspect(ctx context.Context, key string) (info ItemInfo, ok bool, err error)
```

`Get` 读取一个值。

```go
raw, ok, err := mc.Get(ctx, "user:42")
if err != nil { /* infrastructure failure */ }
if !ok { /* miss */ }
```

`GetMany` 对每个后端一次往返批量读取一组 key，返回命中的部分，未命中表现为返回 map 中 key 的缺失。

`Touch(ttl)` 选项让同一条协议命令在读取的同时把命中值的过期时间顺延到 `ttl`，这使 `Get` 成为会话续期的读取一半。

```go
session, ok, err := mc.Get(ctx, "session:"+sid, memcache.Touch(30*time.Minute))
```

这个顺延就是 memcached 原生的 touch，是盲目的：读到什么就延长什么，包括被 `Invalidate` 标记为过时的值。必须立刻生效的撤销要走 `Delete`。

`Inspect` 返回条目的元数据（剩余 TTL、大小、最近访问时间、是否曾被命中），不传输值也不影响它的 LRU 位置，是一个观测工具。

## 写入

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

`Set` 无条件存储一个值，有效期为 `ttl`。`SetMany` 对每个后端一次往返批量存储，所有值共享同一个 `ttl`。

`Add` 只在 key 不存在时存储，并报告本次调用是否成功，可以直接当作简单的分布式锁或只执行一次的保护。

```go
won, err := mc.Add(ctx, "job:daily-report", []byte("1"), 10*time.Minute)
if won { /* this process runs the job */ }
```

`Replace` 只在 key 仍然存在时存储，并报告是否写入了。它是会话续期的写入一半，`false` 表示会话已经结束。

`Touch` 只延长 key 的 TTL，不传输值，是一条盲目的协议命令。key 不存在不算错误。

`Delete` 删除一个 key，删除不存在的 key 视为成功，因为目标状态已经成立。`DeleteMany` 对每个后端一次往返批量删除。

`Invalidate` 把值标记为过时而不是直接删除。在 `grace` 期间读者继续拿到旧值，同时 `Fetch` 会选出一个调用者在后台重新计算；之后这个 key 衰变为正常的未命中。`Invalidate` 与 `Fetch` 管理的 key 配套使用：`grace` 只在没有人续期这个 key 时才是上界，touch 会像顺延普通过期时间一样顺延它。如果旧值一秒都不能再被返回，就用 `Delete`。

## 读取或计算

```go
func (c *Client) Fetch(ctx context.Context, key string, ttl time.Duration,
    loader func(context.Context) ([]byte, error), options ...FetchOption) ([]byte, error)
```

`Fetch` 把最高频的缓存场景做成一个方法。它返回缓存中的值，或者运行 `loader` 计算它并以 `ttl` 存储结果。

```go
report, err := mc.Fetch(ctx, "report:q3", time.Hour, buildReport)
```

未命中时，所有进程中只有一个调用者赢得服务端 lease 并运行 loader。同进程内的其他 goroutine 等待这个结果，其他进程短暂等待后本地计算但不写回。因此值只被计算一次，而不是每个等待者各算一次。

加上 `RefreshAhead(window)` 选项后，剩余 TTL 进入窗口的值会被立即返回，同时选出一个调用者在后台 goroutine 中重新计算，没有任何请求需要承担重算的延迟。

```go
feed, err := mc.Fetch(ctx, "home:"+uid, 5*time.Minute, buildFeed,
    memcache.RefreshAhead(30*time.Second),
)
```

loader 运行在客户端持有的 context 上而不是调用请求的 context 上，因为它的结果可能被其他等待者共享，也可能比调用者活得更久。所有写回都以选举时观察到的版本为条件，因此重算过程中被删除的 key 永远不会被复活。

## 原子修改

```go
func (c *Client) Update(ctx context.Context, key string, ttl time.Duration,
    fn func(current []byte, found bool) ([]byte, error), options ...UpdateOption) ([]byte, error)
```

`Update` 原子地变换一个值。它带版本读出当前值，应用 `fn`，只在中间没有别人改动时写回，冲突则重试。未命中时 `fn` 收到 `(nil, false)`。`fn` 返回错误会中止整个操作且不写入。`fn` 可能运行多次，因此必须是纯函数。如果重试循环一直输给并发写入者，`Update` 返回 `ErrConflict`。

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

`Incr` 给十进制计数器加上 `delta` 并返回新值，未命中时创建计数器，所以第一次请求计为 `delta`。`Decr` 做减法并在零处饱和。由于 `ttl` 在创建时固定，之后的调用不会延长它，这正是固定窗口限流需要的行为。

```go
n, err := mc.Incr(ctx, "rate:"+ip, 1, time.Minute)
```

```go
func (c *Client) Append(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Prepend(ctx context.Context, key string, fragment []byte, ttl time.Duration) error
func (c *Client) Take(ctx context.Context, key string) ([]byte, error)
```

`Append` 和 `Prepend` 把原始字节拼接到值的尾部或头部，未命中时创建值。`Take` 原子地读出一个值并删除它，不存在并发追加的字节丢失的窗口。两者合起来构成简单的收集再取走模式，比如缓冲事件并定期取走一批。`Take` 返回 `nil` 表示没有东西可取。

## 故障策略

默认情况下每个基础设施故障都以错误形式返回。客户端选项 `Degrade(true)` 让读操作把故障报告为未命中，让无条件写操作静默放弃，因为缓存故障不应该变成整站故障。每个被吸收的错误仍然会到达 `OnError` 钩子。

```go
mc, err := memcache.NewServers(servers,
    memcache.Degrade(true),
    memcache.OnError(func(err error) { log.Print(err) }),
)
```

结果用于业务判断的方法（`Add`、`Replace`、`Update`、`Incr`、`Decr`、`Take`）在 `Degrade` 下仍然报错，`AmbiguousWriteError`（写入可能已经生效）也总是返回。写入开始后客户端永远不会自动重试命令，因为盲目重试算术或追加可能让变更生效两次。

## 协议层

场景方法没有覆盖的一切都在 `Meta()` 后面，它是 meta 协议的 1:1 映射，返回类型化的结果而不是把协议状态折叠成错误。

```go
func (m *MetaClient) Get(ctx context.Context, key string, options GetOptions) (GetResult, error)
func (m *MetaClient) Set(ctx context.Context, key string, value []byte, options SetOptions) (MutationResult, error)
func (m *MetaClient) Delete(ctx context.Context, key string, options DeleteOptions) (MutationResult, error)
func (m *MetaClient) Arithmetic(ctx context.Context, key string, options ArithmeticOptions) (ArithmeticResult, error)
func (m *MetaClient) Execute(ctx context.Context, command MetaCommand) (RawResponse, error)
func (m *MetaClient) Batch(ctx context.Context, operations []Operation) ([]OperationResult, error)
func (m *MetaClient) Debug(ctx context.Context, key string) (map[string]string, error)
func (m *MetaClient) Noop(ctx context.Context) error
```

```go
result, err := mc.Meta().Get(ctx, key, memcache.GetOptions{ReturnCAS: true, ReturnTTL: true})
raw, err := mc.Meta().Execute(ctx, memcache.MetaCommand{Command: "mg", Key: key, Flags: []string{"v", "t"}})
```

`Batch` 在写出前校验每个操作，按服务器分组，用 quiet 命令流水线执行。结果保持输入顺序，某个后端的故障只记录在该后端的结果上。包含空白或控制字节的 key 会自动用 meta 的 `b` 标志做 base64 编码。

## 许可证

MIT
