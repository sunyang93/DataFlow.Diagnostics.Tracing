using DataFlow.Diagnostics.Tracing;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.IntegrationTests
{
    /// <summary>
    /// TraceService.RemoveExpiredTraceIndexMembersAsync 集成测试。
    /// 连接本地真实 Redis，向索引 SortedSet 写入"已过期"和"未过期"两种 score 的 member，
    /// 验证清理方法仅移除 score 早于保留期阈值的 member，未过期的保留。
    /// Redis 不可达时用例自动 Skip。
    /// </summary>
    [Collection("RedisIntegration")]
    public class TraceServiceCleanupIntegrationTests : IAsyncLifetime
    {
        private readonly RedisTestFixture _fixture;
        private readonly IDatabase _db;
        private readonly IServer _server;
        private readonly TraceService _service;
        private readonly string _objectId;
        private readonly string _indexKey;

        public TraceServiceCleanupIntegrationTests(RedisTestFixture fixture)
        {
            _fixture = fixture;
            _db = fixture.IsAvailable ? fixture.GetDatabase() : null!;
            _server = fixture.IsAvailable ? fixture.GetServer() : null!;
            _objectId = $"it-clean-{Guid.NewGuid():N}";
            _indexKey = TraceDefinitions.SortedSetKey(TraceDefinitions.ChannelTraceObject, _objectId);
            _service = fixture.IsAvailable ? new TraceService(_db, TimeProvider.System) : null!;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            await _fixture.CleanupKeysAsync($"trace:*:{_objectId}:*").ConfigureAwait(false);
        }

        [RedisFact(DisplayName = "清理集成 - 仅移除 score 早于保留期阈值的索引 member，未过期的保留")]
        public async Task RemoveExpiredTraceIndexMembersAsync_RemovesOnlyExpiredMembers()
        {
            const long retentionMs = 60 * 1000; // 保留期 1 分钟
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // 3 条已过期（180s / 120s / 90s 前），2 条未过期（30s 前 / 当前）
            var expiredDocKeys = new[]
            {
                TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-old-1"),
                TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-old-2"),
                TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-old-3"),
            };
            var freshDocKeys = new[]
            {
                TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-new-1"),
                TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-new-2"),
            };

            await _db.SortedSetAddAsync(_indexKey, expiredDocKeys[0], nowMs - 180 * 1000);
            await _db.SortedSetAddAsync(_indexKey, expiredDocKeys[1], nowMs - 120 * 1000);
            await _db.SortedSetAddAsync(_indexKey, expiredDocKeys[2], nowMs - 90 * 1000);
            await _db.SortedSetAddAsync(_indexKey, freshDocKeys[0], nowMs - 30 * 1000);
            await _db.SortedSetAddAsync(_indexKey, freshDocKeys[1], nowMs);

            // 全局扫描可能包含其他测试遗留的过期 member，因此删除总数 >= 3
            var removed = await _service.RemoveExpiredTraceIndexMembersAsync(_server, retentionMs);

            Assert.True(removed >= 3, $"应至少移除本测试写入的 3 个过期 member，实际移除 {removed}。");

            // 本索引应只剩 2 条未过期 member
            var remainingCount = await _db.SortedSetLengthAsync(_indexKey);
            Assert.Equal(2, remainingCount);

            var remainingMembers = (await _db.SortedSetRangeByScoreAsync(_indexKey))
                .Select(v => v.ToString()).ToArray();
            foreach (var fresh in freshDocKeys)
            {
                Assert.Contains(fresh, remainingMembers);
            }
            foreach (var expired in expiredDocKeys)
            {
                Assert.DoesNotContain(expired, remainingMembers);
            }
        }

        [RedisFact(DisplayName = "清理集成 - 全部 member 均未过期时不移除任何 member，返回 0 之外不影响索引")]
        public async Task RemoveExpiredTraceIndexMembersAsync_AllFresh_RemovesNothing()
        {
            const long retentionMs = 60 * 1000;
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            var docKey1 = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-fresh-1");
            var docKey2 = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, _objectId, "doc-fresh-2");
            await _db.SortedSetAddAsync(_indexKey, docKey1, nowMs - 10 * 1000);
            await _db.SortedSetAddAsync(_indexKey, docKey2, nowMs);

            // 本索引没有过期 member；全局可能移除其他键的过期 member，这里只验证本索引条数不变
            await _service.RemoveExpiredTraceIndexMembersAsync(_server, retentionMs);

            Assert.Equal(2, await _db.SortedSetLengthAsync(_indexKey));
        }
    }
}
