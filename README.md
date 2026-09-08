# DataFlow.Diagnostics.Tracing

基于 **Redis（RedisJSON + SortedSet）** 实现的分布式链路追踪类库，支持 **Channel（通道）** 和 **Pipeline（管道）** 两种追踪对象类型，提供高吞吐写入与按时间范围分页查询能力。

## 特性

- **文档存储 + 时间索引**：RedisJSON 存储原始 JSON 文档，SortedSet 以时间戳为 score 建立时间索引，支持按时间范围分页查询。
- **单条写入原子性**：使用 Redis 事务（`MULTI/EXEC`）保证 JSON 文档、索引写入与过期时间设置的原子性。
- **批量高吞吐写入**：Batch 管道打包 + `SemaphoreSlim` 并发限流 + 分批提交，兼顾吞吐与资源占用。
- **Fire-and-Forget 模式**：高频写入场景下可选"火并忘记"，立即返回且异常仅内部观察。
- **TTL 自动过期**：所有 key 均设置过期时间（默认 2 小时），索引随写入滑动续期，避免死数据残留。
- **可测试性**：依赖 `TimeProvider` 抽象时间源，支持单元测试注入可控时间。

## 项目结构

| 项目 | 说明 |
|---|---|
| `DataFlow.Diagnostics.Tracing` | 核心类库（netstandard2.1） |
| `DataFlow.Diagnostics.Tracing.Tests` | 单元测试（xUnit + Moq，net10.0） |
| `DataFlow.Diagnostics.Tracing.IntegrationTests` | 集成测试（真实 Redis，net10.0） |

## 存储设计

| 数据 | Redis Key 格式 | 说明 |
|---|---|---|
| JSON 文档 | `trace:{traceObject}:{objectId}:{traceItemId}` | RedisJSON，存储完整 Trace 内容 |
| 时间索引 | `trace:{traceObject}:{objectId}:index` | SortedSet，member=文档 Key，score=Unix 毫秒时间戳 |

- `traceObject`：追踪对象类型，`channel` 或 `pipeline`。
- `objectId`：业务对象 ID（通道 ID / 管道 ID）。
- `traceItemId`：单条追踪项唯一 ID（建议使用 Guid）。

## 快速开始

### 环境要求

- Redis 服务端需支持 **RedisJSON** 模块（如 Redis Stack）。
- .NET Standard 2.1+ 兼容运行时。

### 安装依赖

```bash
dotnet add package NRedisStack
dotnet add package Microsoft.Bcl.TimeProvider
```

### 写入 Trace

```csharp
using StackExchange.Redis;
using DataFlow.Diagnostics.Tracing;

var multiplexer = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
var database = multiplexer.GetDatabase();
ITraceService traceService = new TraceService(database);

// 单条写入（Channel）
await traceService.WriteChannelTraceAsync(new ChannelTraceWriteParameter(
    channelId: "channel-1",
    traceItemId: Guid.NewGuid().ToString(),
    value: """{"event":"start","status":"running"}"""));

// 批量写入（Pipeline），分批 + 限流并发
await traceService.WritePipelineTracesAsync(
    traces.Select(t => new PipelineTraceWriteParameter(t.PipelineId, t.TraceItemId, t.Json)),
    new TraceWriteOptions
    {
        RetentionTimeMilliseconds = 4 * 60 * 60 * 1000, // 自定义保留 4 小时
        FireAndForget = false
    });
```

### 查询 Trace

```csharp
// 按时间范围分页查询（返回索引条目，不含 JSON 内容）
var result = await traceService.QueryChannelTraceRangeAsync(new ChannelRangeTracesQueryParameter(
    objectId: "channel-1",
    startTime: DateTimeOffset.UtcNow.AddHours(-1),
    endTime: null,          // null 时默认为当前 UTC 时间
    pageindex: 1,
    pagesize: 20));

Console.WriteLine($"总数: {result.TotalCount}");
foreach (var trace in result.Traces)
{
    Console.WriteLine($"{trace.TraceTime:O} -> {trace.TraceDocumentJsonKey}");
}

// 按文档 Key 获取完整 JSON 内容
var document = await traceService.QueryChannelTraceDocumentAsync("channel-1", traceItemId);
Console.WriteLine(document?.TraceDocumentJson);
```

## API 概览

| 方法 | 说明 |
|---|---|
| `WriteChannelTraceAsync` / `WritePipelineTraceAsync` | 单条写入（Redis 事务保证原子性） |
| `WriteChannelTracesAsync` / `WritePipelineTracesAsync` | 批量写入（分批 + 限流 + 管道） |
| `QueryChannelTraceRangeAsync` / `QueryPipelineTraceRangeAsync` | 按时间范围分页查询索引条目 |
| `QueryChannelTraceDocumentAsync` / `QueryPipelineTraceDocumentAsync` | 查询单条 Trace 完整 JSON 文档 |
| `QueryTraceAsync(documentKey)` | 按文档 Key 直接查询 JSON 内容 |
| `RemoveExpiredTraceIndexMembersAsync(server, retentionMilliseconds?)` | SCAN 所有索引键，Batch 批量移除 score 早于保留期阈值的过期 member（返回移除总数） |

### 清理过期索引

持续写入会使索引键随写入滑动续期，导致 SortedSet 中残留指向已过期文档的 member。可通过定时任务调用清理方法（需传入 `IServer` 用于 SCAN 扫描）：

```csharp
var server = multiplexer.GetServer(multiplexer.GetEndPoints()[0]);

// retentionMilliseconds 为 null 时使用默认保留期（2 小时）
long removed = await traceService.RemoveExpiredTraceIndexMembersAsync(server);
```

### TraceService 构造参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `database` | - | Redis 数据库实例（必填） |
| `timeProvider` | `TimeProvider.System` | 时间源，测试可注入可控时间 |
| `writeConcurrency` | `20` | 批量写入最大并发批次数 |
| `batchSize` | `40` | 每批次 Trace 条目数 |

### TraceWriteOptions

| 属性 | 默认值 | 说明 |
|---|---|---|
| `RetentionTimeMilliseconds` | `null`（2 小时） | 文档与索引 key 的过期时间（毫秒） |
| `FireAndForget` | `false` | `true` 时立即返回，不等待 Redis 应答，异常仅内部观察 |

## 运行测试

### 单元测试（无需 Redis，使用 Moq 模拟）

```bash
dotnet test DataFlow.Diagnostics.Tracing.Tests
```

### 集成测试（需要本地 Redis）

默认连接 `localhost:6379`，可通过环境变量覆盖：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TRACE_REDIS_CONNECTION` | `localhost:6379` | Redis 连接串 |
| `TRACE_REDIS_KEEP_DATA` | 未设置 | 设为 `1` 时保留测试数据（调试用） |

Redis 不可用时集成测试会自动标记为 **Skipped**，不会导致测试失败。

```bash
dotnet test DataFlow.Diagnostics.Tracing.IntegrationTests
```
