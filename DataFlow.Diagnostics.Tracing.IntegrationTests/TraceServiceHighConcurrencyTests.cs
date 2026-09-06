using DataFlow.Diagnostics.Tracing;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Assert = Xunit.Assert;

namespace DataFlow.Diagnostics.Tracing.IntegrationTests
{
    /// <summary>
    /// TraceService 高并发写入集成测试。
    /// 连接本地真实 Redis，通过 Task.WhenAll 触发并发写入，验证：
    ///   - 多 Key 并发写入无丢失、索引条数正确
    ///   - 同 Key 并发覆盖写入最终一致、索引不重复
    ///   - 批量 + 单条混合并发数据完整
    ///   - FireAndForget 高并发最终一致
    ///   - 大批量单次写入全部落库
    ///   - 并发写入下 TTL 正确设置
    /// Redis 不可达时全部用例自动 Skip。
    /// </summary>
    [Collection("RedisIntegration")]
    public class TraceServiceHighConcurrencyTests : IAsyncLifetime
    {
        private readonly RedisTestFixture _fixture;
        private readonly IDatabase _db;
        private readonly TraceService _service;
        private readonly string _channelId;
        private readonly string _pipelineId;

        public TraceServiceHighConcurrencyTests(RedisTestFixture fixture)
        {
            // Redis 不可用时 RedisFactAttribute 已在发现阶段标记 Skip，
            // 此处仅做防御式初始化：不可用则不创建服务实例，避免空引用。
            _fixture = fixture;
            _db = fixture.IsAvailable ? fixture.GetDatabase() : null!;
            _channelId = $"it-conc-ch-{Guid.NewGuid():N}";
            _pipelineId = $"it-conc-pl-{Guid.NewGuid():N}";
            // 使用系统真实时间源，更贴近生产高并发场景
            _service = fixture.IsAvailable ? new TraceService(_db, TimeProvider.System) : null!;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        /// <summary>每个测试结束后清理本实例写入的所有 Key（文档与索引）。</summary>
        public async Task DisposeAsync()
        {
            await _fixture.CleanupKeysAsync($"trace:*:{_channelId}:*").ConfigureAwait(false);
            await _fixture.CleanupKeysAsync($"trace:*:{_pipelineId}:*").ConfigureAwait(false);
        }

        #region 辅助

        private string IndexKey(string traceObject, string objectId)
            => TraceDefinitions.SortedSetKey(traceObject, objectId);

        private async Task<string?> JsonGetAsync(string docKey)
        {
            var result = await _db.ExecuteAsync("JSON.GET", docKey).ConfigureAwait(false);
            return result.IsNull ? null : result.ToString();
        }

        /// <summary>轮询等待索引条数达到预期，最多等待 timeout。</summary>
        private async Task<bool> WaitForIndexCountAsync(string indexKey, long expected, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                var count = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
                if (count >= expected) return true;
                await Task.Delay(20).ConfigureAwait(false);
            }
            return false;
        }

        #endregion

        #region 多 Key 高并发写入

        [RedisFact(DisplayName = "[高并发] 多 Key 并发单条写入 100 条：全部落库，索引条数=100，JSON 内容正确")]
        public async Task WriteChannelTraceAsync_ConcurrentDistinctKeys_AllPersisted()
        {
            // Arrange
            const int count = 100;
            var items = Enumerable.Range(0, count)
                .Select(i => (Id: $"item-{i:D3}", Json: $"{{\"i\":{i}}}"))
                .ToList();

            // Act: 100 个任务并发写入不同 Key
            var tasks = items.Select(t =>
                _service.WriteChannelTraceAsync(
                    new ChannelTraceWriteParameter(_channelId, t.Id, t.Json)));
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // Assert: 索引条数准确
            var indexKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var total = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
            Assert.Equal(count, total);

            // Assert: 每个文档内容正确
            foreach (var (id, json) in items)
            {
                var docKey = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _channelId, id);
                var stored = await JsonGetAsync(docKey).ConfigureAwait(false);
                Assert.NotNull(stored);
                Assert.Contains(json, stored);
            }
        }

        #endregion

        #region 同 Key 覆盖写入

        [RedisFact(DisplayName = "[高并发] 同 Key 并发覆盖写入：索引不重复（member 去重），最终文档存在")]
        public async Task WriteChannelTraceAsync_ConcurrentSameKey_IndexDeduplicated()
        {
            // Arrange: 50 个任务并发写同一个 traceItemId，JSON 内容不同
            const int writers = 50;
            const string sharedItemId = "shared-item";
            var tasks = Enumerable.Range(0, writers).Select(i =>
                _service.WriteChannelTraceAsync(
                    new ChannelTraceWriteParameter(_channelId, sharedItemId, $"{{\"seq\":{i}}}")));

            // Act
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // Assert: SortedSet member 相同，索引只有 1 条
            var indexKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var total = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
            Assert.Equal(1, total);

            // Assert: 文档存在，内容是某个合法 seq
            var docKey = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _channelId, sharedItemId);
            var stored = await JsonGetAsync(docKey).ConfigureAwait(false);
            Assert.NotNull(stored);
            Assert.Contains("\"seq\":", stored);
        }

        #endregion

        #region 批量 + 单条混合并发

        [RedisFact(DisplayName = "[高并发] 批量 + 单条混合并发：所有数据无丢失落库")]
        public async Task WriteMixed_BatchAndSingleConcurrently_NoLoss()
        {
            // Arrange
            const int batchCount = 10;       // 10 个批量任务
            const int batchSize = 15;        // 每批 15 条
            const int singleCount = 50;      // 50 个单条任务
            var expectedTotal = batchCount * batchSize + singleCount;

            // 批量任务：每个批量任务写 15 条不同 itemId
            var batchTasks = Enumerable.Range(0, batchCount).Select(b =>
            {
                var traces = Enumerable.Range(0, batchSize)
                    .Select(i => new ChannelTraceWriteParameter(
                        _channelId, $"b-{b}-{i}", $"{{\"b\":{b},\"i\":{i}}}"))
                    .ToList();
                return _service.WriteChannelTracesAsync(traces);
            });

            // 单条任务：写 50 条不同 itemId
            var singleTasks = Enumerable.Range(0, singleCount).Select(i =>
                _service.WriteChannelTraceAsync(
                    new ChannelTraceWriteParameter(_channelId, $"s-{i}", $"{{\"s\":{i}}}")));

            // Act: 批量与单条交错并发
            await Task.WhenAll(batchTasks.Concat(singleTasks)).ConfigureAwait(false);

            // Assert: 总数准确，无丢失
            var indexKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var total = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
            Assert.Equal(expectedTotal, total);
        }

        #endregion

        #region FireAndForget 高并发最终一致

        [RedisFact(DisplayName = "[高并发] FireAndForget 高并发 200 条：最终全部落库（最终一致）")]
        public async Task WriteChannelTracesAsync_FireAndForgetHighConcurrency_EventuallyAllPersisted()
        {
            // Arrange
            const int count = 200;
            var traces = Enumerable.Range(0, count)
                .Select(i => new ChannelTraceWriteParameter(_channelId, $"faf-{i:D3}", $"{{\"i\":{i}}}"))
                .ToList();
            var options = new TraceWriteOptions { FireAndForget = true };

            // Act: F&F 立即返回，不等待 Redis 应答
            await _service.WriteChannelTracesAsync(traces, options).ConfigureAwait(false);

            // Assert: 轮询等待最终全部落库（最多 5 秒）
            var indexKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var reached = await WaitForIndexCountAsync(indexKey, count, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.True(reached, $"FireAndForget 写入未在超时内全部落库，最终条数={await _db.SortedSetLengthAsync(indexKey)}");

            var finalCount = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
            Assert.Equal(count, finalCount);
        }

        #endregion

        #region 大批量单次写入

        [RedisFact(DisplayName = "[高并发] 单次批量写入 1000 条：全部落库，索引条数=1000")]
        public async Task WriteChannelTracesAsync_LargeBatch_1000Items_AllPersisted()
        {
            // Arrange: 1000 条，batchSize 默认 40，将分 25 批由内部 Semaphore 限流并发执行
            const int count = 1000;
            var traces = Enumerable.Range(0, count)
                .Select(i => new ChannelTraceWriteParameter(_channelId, $"big-{i:D4}", $"{{\"i\":{i}}}"))
                .ToList();

            // Act
            await _service.WriteChannelTracesAsync(traces).ConfigureAwait(false);

            // Assert: 全部落库
            var indexKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var total = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
            Assert.Equal(count, total);

            // 抽样验证首尾文档内容
            var firstKey = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _channelId, "big-0000");
            var lastKey = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _channelId, "big-0999");
            Assert.NotNull(await JsonGetAsync(firstKey).ConfigureAwait(false));
            Assert.NotNull(await JsonGetAsync(lastKey).ConfigureAwait(false));
        }

        #endregion

        #region 并发写入下 TTL 正确性

        [RedisFact(DisplayName = "[高并发] 并发写入下自定义 TTL 正确设置：所有 Key TTL 在 (0, retention] 区间")]
        public async Task WriteChannelTraceAsync_ConcurrentCustomRetention_TtlCorrect()
        {
            // Arrange
            const int count = 50;
            const long retentionMs = 60_000; // 1 分钟
            var options = new TraceWriteOptions { RetentionTimeMilliseconds = retentionMs };
            var itemIds = Enumerable.Range(0, count).Select(i => $"ttl-{i:D2}").ToList();

            // Act: 并发写入，自定义保留时间
            var tasks = itemIds.Select(id =>
                _service.WriteChannelTraceAsync(
                    new ChannelTraceWriteParameter(_channelId, id, "{}"), options));
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // Assert: 随机抽 10 个文档 Key + 索引 Key，TTL 均在 (0, retentionMs]
            var sample = itemIds.OrderBy(_ => Guid.NewGuid()).Take(10).ToList();
            foreach (var id in sample)
            {
                var docKey = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _channelId, id);
                var ttl = await _db.KeyTimeToLiveAsync(docKey).ConfigureAwait(false);
                Assert.NotNull(ttl);
                Assert.True(ttl!.Value.TotalMilliseconds > 0, $"doc {id} TTL 应大于 0");
                Assert.True(ttl.Value.TotalMilliseconds <= retentionMs,
                    $"doc {id} TTL {ttl.Value.TotalMilliseconds} 应 <= {retentionMs}");
            }

            var idxKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var idxTtl = await _db.KeyTimeToLiveAsync(idxKey).ConfigureAwait(false);
            Assert.NotNull(idxTtl);
            Assert.True(idxTtl!.Value.TotalMilliseconds > 0);
            Assert.True(idxTtl.Value.TotalMilliseconds <= retentionMs);
        }

        #endregion

        #region 并发异常隔离

        [RedisFact(DisplayName = "[高并发] 部分参数非法时不影响其它并发写入：合法数据仍落库")]
        public async Task WriteChannelTraceAsync_ConcurrentWithInvalidItem_ValidOnesPersisted()
        {
            // Arrange: 合法 30 条 + 非法 5 条（空 Value）
            // 注意：WriteChannelTraceAsync 在进入异步前会同步执行 ValidateKeyArguments 校验，
            // 非法参数会在调用点立即抛出 ArgumentException，而非返回 faulted Task。
            const int validCount = 30;
            var exceptions = new List<Exception>();

            // 合法任务：延迟构造，在 WhenAll 时才真正调用
            var validTasks = Enumerable.Range(0, validCount).Select(i =>
                _service.WriteChannelTraceAsync(
                    new ChannelTraceWriteParameter(_channelId, $"ok-{i:D2}", $"{{\"i\":{i}}}")));

            // 非法任务：在调用点用 try/catch 捕获同步抛出的 ArgumentException
            var invalidTasks = Enumerable.Range(0, 5).Select(i =>
            {
                try
                {
                    return _service.WriteChannelTraceAsync(
                        new ChannelTraceWriteParameter(_channelId, $"bad-{i}", string.Empty));
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    return Task.CompletedTask;
                }
            });

            // Act: 合法项异步执行，非法项在构造时已同步抛异常并被捕获
            await Task.WhenAll(validTasks.Concat(invalidTasks)).ConfigureAwait(false);

            // Assert: 5 个非法项抛异常，30 个合法项全部落库
            Assert.Equal(5, exceptions.Count);
            Assert.All(exceptions, ex => Assert.IsType<ArgumentException>(ex));

            var indexKey = IndexKey(TraceDefinitions.ChannelTraceObject, _channelId);
            var total = await _db.SortedSetLengthAsync(indexKey).ConfigureAwait(false);
            Assert.Equal(validCount, total);
        }

        #endregion
    }
}
