# memcache

[English](README.md) | 简体中文

`memcache` 是一个面向 memcached 现代 meta 文本协议的并发 Go 客户端。它实现了 `mg`、`ms`、`md`、`ma`、`me` 和 `mn`，不包含旧版 get/set 协议的实现。

这个客户端以场景为导向：每个用户场景对应一个动词，建立在四条公理之上。未命中是一种正常的回答，而不是错误。每个缓存值都有权威数据源和重算路径，因此「获取或计算」是一等动词。并发协调（租约、compare-and-swap 循环、请求合并）是库的职责，永远不会出现在调用方代码里。缓存是一种可用性优化，因此失败行为是一项显式策略。

当前版本中值的类型是 `[]byte`，序列化交由调用方处理，直到 Go 支持泛型方法（go1.27）。届时同样的动词会获得类型参数，而形态保持不变。

## 读和写

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

每个存值的操作都把 TTL 作为位置参数，与 go-redis 的惯例一致。没有客户端级的默认 TTL：每个调用点都能直接看到数据的生存期，存储永不过期的值是一个显式选择 `memcache.Forever`。

```go
err = mc.Set(ctx, "config:site", buf, memcache.Forever)
```

在 `Incr`/`Decr`/`Append`/`Prepend` 上，TTL 只在本次调用自动创建 key 时生效，永远不会延长已存在的 key 的生存期。

可选修饰按动词做了类型约束：`RefreshAhead` 只被 `Fetch` 接受。把选项用在没有意义的动词上是编译错误，而不是运行时意外。

## 获取或计算：Fetch

`Fetch` 把最高频的缓存场景浓缩成一个动词：返回缓存值，或者恰好计算一次。

```go
report, err := mc.Fetch(ctx, "report:q3", time.Hour, buildReport)
```

未命中时，所有进程中只有一个调用者赢得服务端租约（meta vivify）并运行加载函数。同进程内的其他 goroutine 等待这个结果，其他进程短暂等待后本地计算且不写回。启用 `RefreshAhead` 后，临近过期的值会被立即返回，同时选出一个调用者在后台 goroutine 中重算，因此没有任何请求需要承担重算延迟：

```go
feed, err := mc.Fetch(ctx, "home:"+uid, 5*time.Minute, buildFeed,
    memcache.RefreshAhead(30*time.Second),
)
```

`Invalidate(ctx, key, grace)` 把值标记为陈旧而不是删除。宽限期内读取方继续拿到旧副本，同时 `Fetch` 选出一个调用者在后台刷新。`Delete` 是硬删除的变体。所有写回都以选举时观察到的版本为条件，因此重算过程中被删除的键永远不会复活。

## 并发修改：Update 和 Take

`Update` 在内部执行「读、变换、条件写、重试」循环，版本令牌永远不会出现在用户代码中：

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

`Append`/`Prepend` 把原始字节拼接到一个值上，`Take` 原子地读出并删除它，不存在并发追加的字节丢失的窗口。库只提供这个机制，字节如何组织、攒起来做什么用由调用方决定。`Incr`/`Decr` 自动创建计数器时以创建时刻的 TTL 为准，后续递增不再延长它，这正是固定窗口限流：

```go
n, err := mc.Incr(ctx, "rate:"+ip, 1, time.Minute)
```

## 失败策略

默认情况下每个基础设施失败都以错误形式暴露。`Degrade(true)` 让读操作把失败报告为未命中，无条件写操作静默放弃，因为缓存故障不应该变成站点故障。每个被吸收的错误仍会到达 `OnError` 钩子。答案会影响业务决策的动词（`Add`、`Replace`、`Update`、`Incr`、`Decr`、`Take`）依然大声失败，`AmbiguousWriteError`（写入可能已落地）也总是会暴露。

```go
mc, err := memcache.NewServers(servers,
    memcache.Degrade(true),
    memcache.OnError(func(err error) { log.Print(err) }),
)
```

客户端在开始写入之后永远不会自动重试命令，盲目重试算术或追加操作可能让变更被应用两次。

## 协议层

场景动词没有覆盖的一切都在 `Meta()` 后面，它与 meta 协议 1:1 对应，返回带类型的结果而不是把协议状态压扁成错误：

```go
result, err := mc.Meta().Get(ctx, key, memcache.GetOptions{ReturnCAS: true, ReturnTTL: true})
raw, err := mc.Meta().Execute(ctx, memcache.MetaCommand{Command: "mg", Key: key, Flags: []string{"v", "t"}})
```

`Meta().Batch` 在写入前校验每个操作，按服务器分组，并用 quiet 命令加 `mn` 屏障做流水线。结果保持输入顺序，某个后端的失败只记录在该后端的结果上。包含空白或控制字节的键会自动用 meta 的 `b` 标志做 base64 编码。

多服务器默认使用稳定的会合哈希（rendezvous hashing），`WithRouter` 可以替换它。每个服务器有一个弹性的并发连接池：`WithMaxIdleConns` 限制保留的空闲连接数，而不是活跃请求数。空闲连接按最近释放优先复用，空闲超过 `WithIdleTimeout`（默认 90 秒）后会重新拨号。

```go
mc, err := memcache.NewServers([]string{"cache-a:11211", "cache-b:11211"})
```

## 许可证

MIT
