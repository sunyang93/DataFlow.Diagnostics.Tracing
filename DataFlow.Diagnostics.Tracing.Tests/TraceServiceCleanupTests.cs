using DataFlow.Diagnostics.Tracing;
using Moq;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// TraceService 过期索引清理方法（RemoveExpiredTraceIndexMembersAsync）的单元测试（partial 类）。
    /// 使用 Moq 模拟 IServer（SCAN 扫描索引键）和 IBatch（ZREMRANGEBYSCORE 管道批处理），
    /// 使用可控的 TimeProvider 提供固定时间源，验证扫描模式、分数边界、分批和返回总数。
    /// </summary>
    public partial class TraceServiceTests
    {
        /// <summary>默认保留期下的过期阈值：FixedUtcNowMs - 2 小时。</summary>
        private const long ExpectedDefaultCutoffMs = FixedUtcNowMs - TraceDefinitions.DefaultRetentionTime;

        /// <summary>将数组包装为 IAsyncEnumerable，模拟 IServer.KeysAsync 的 SCAN 游标结果。</summary>
        private static async IAsyncEnumerable<RedisKey> YieldKeys(params RedisKey[] keys)
        {
            foreach (var key in keys)
            {
                yield return key;
            }
            await Task.CompletedTask;
        }

        /// <summary>创建返回指定索引键集合的 IServer mock。</summary>
        private static Mock<IServer> CreateServerMock(params RedisKey[] scannedKeys)
        {
            var serverMock = new Mock<IServer>();
            serverMock.Setup(s => s.KeysAsync(
                    It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                    It.IsAny<long>(), It.IsAny<int>(), CommandFlags.None))
                .Returns(YieldKeys(scannedKeys));
            return serverMock;
        }

        /// <summary>创建配置了 SortedSetRemoveRangeByScoreAsync 的 IBatch mock，每个键返回 removedPerKey。</summary>
        private static Mock<IBatch> CreateCleanupBatchMock(long removedPerKey)
        {
            var batchMock = new Mock<IBatch>();
            batchMock.Setup(b => b.SortedSetRemoveRangeByScoreAsync(
                    It.IsAny<RedisKey>(), It.IsAny<double>(), It.IsAny<double>(),
                    It.IsAny<Exclude>(), CommandFlags.None))
                .ReturnsAsync(removedPerKey);
            batchMock.Setup(b => b.Execute());
            return batchMock;
        }

        #region 参数校验

        [Fact(DisplayName = "RemoveExpiredTraceIndexMembersAsync - server 为 null 抛出 ArgumentNullException")]
        public async Task RemoveExpiredTraceIndexMembersAsync_NullServer_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() =>
                service.RemoveExpiredTraceIndexMembersAsync(null!));
            Assert.Equal("server", ex.ParamName);
        }

        [Theory(DisplayName = "RemoveExpiredTraceIndexMembersAsync - retentionMilliseconds <= 0 抛出 ArgumentOutOfRangeException（不触发扫描和批处理）")]
        [InlineData(0)]
        [InlineData(-100)]
        public async Task RemoveExpiredTraceIndexMembersAsync_InvalidRetention_Throws(long retention)
        {
            // Strict 模式：任何 Redis 调用都会立即失败，证明校验先于 SCAN / Batch
            var serverMock = new Mock<IServer>(MockBehavior.Strict);
            var dbMock = new Mock<IDatabase>(MockBehavior.Strict);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                service.RemoveExpiredTraceIndexMembersAsync(serverMock.Object, retention));
            Assert.Equal("retentionMilliseconds", ex.ParamName);
        }

        #endregion

        #region 正常流程

        [Fact(DisplayName = "清理 - 扫描不到索引键时返回 0，且不创建 Batch")]
        public async Task RemoveExpiredTraceIndexMembersAsync_NoIndexKeys_ReturnsZeroWithoutBatch()
        {
            var serverMock = CreateServerMock();
            var dbMock = new Mock<IDatabase>();
            dbMock.SetupGet(d => d.Database).Returns(0);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var removed = await service.RemoveExpiredTraceIndexMembersAsync(serverMock.Object);

            Assert.Equal(0, removed);
            dbMock.Verify(d => d.CreateBatch(It.IsAny<object?>()), Times.Never);
            serverMock.Verify(s => s.KeysAsync(
                0, TraceDefinitions.IndexKeyPattern, It.IsAny<int>(),
                It.IsAny<long>(), It.IsAny<int>(), CommandFlags.None), Times.Once);
        }

        [Fact(DisplayName = "清理 - 3 个索引键：1 个 Batch、3 次 ZREMRANGEBYSCORE，边界为 (-inf, cutoff) 且不含上限，返回删除总数")]
        public async Task RemoveExpiredTraceIndexMembersAsync_ThreeKeys_BatchRemovesWithCorrectBounds()
        {
            var indexKey1 = TraceDefinitions.SortedSetKey(TraceDefinitions.ChannelTraceObject, "ch-1");
            var indexKey2 = TraceDefinitions.SortedSetKey(TraceDefinitions.ChannelTraceObject, "ch-2");
            var indexKey3 = TraceDefinitions.SortedSetKey(TraceDefinitions.PipelineTraceObject, "p-1");

            var serverMock = CreateServerMock(indexKey1, indexKey2, indexKey3);

            var dbMock = new Mock<IDatabase>();
            dbMock.SetupGet(d => d.Database).Returns(0);
            var batchMock = CreateCleanupBatchMock(removedPerKey: 2);
            dbMock.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(batchMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var total = await service.RemoveExpiredTraceIndexMembersAsync(serverMock.Object);

            Assert.Equal(6, total);
            batchMock.Verify(b => b.Execute(), Times.Once);
            // 严格删除 score < cutoff：下限 -inf（包含），上限 cutoff（Exclude.Stop 排除）
            batchMock.Verify(b => b.SortedSetRemoveRangeByScoreAsync(
                It.IsAny<RedisKey>(), double.NegativeInfinity, ExpectedDefaultCutoffMs,
                Exclude.Stop, CommandFlags.None), Times.Exactly(3));
            // SCAN 使用 trace:*:index 模式
            serverMock.Verify(s => s.KeysAsync(
                0, TraceDefinitions.IndexKeyPattern, It.IsAny<int>(),
                It.IsAny<long>(), It.IsAny<int>(), CommandFlags.None), Times.Once);
        }

        [Fact(DisplayName = "清理 - 自定义保留期：cutoff = 固定当前时间 - 自定义保留期")]
        public async Task RemoveExpiredTraceIndexMembersAsync_CustomRetention_UsesCustomCutoff()
        {
            const long customRetentionMs = 30 * 60 * 1000; // 30 分钟
            var expectedCutoffMs = FixedUtcNowMs - customRetentionMs;
            var indexKey = TraceDefinitions.SortedSetKey(TraceDefinitions.ChannelTraceObject, "ch-9");

            var serverMock = CreateServerMock(indexKey);
            var dbMock = new Mock<IDatabase>();
            dbMock.SetupGet(d => d.Database).Returns(0);
            var batchMock = CreateCleanupBatchMock(removedPerKey: 1);
            dbMock.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(batchMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var total = await service.RemoveExpiredTraceIndexMembersAsync(serverMock.Object, customRetentionMs);

            Assert.Equal(1, total);
            batchMock.Verify(b => b.SortedSetRemoveRangeByScoreAsync(
                indexKey, double.NegativeInfinity, expectedCutoffMs,
                Exclude.Stop, CommandFlags.None), Times.Once);
        }

        [Fact(DisplayName = "清理 - 索引键数量超过 batchSize 时按批次创建多个 Batch 并汇总删除数")]
        public async Task RemoveExpiredTraceIndexMembersAsync_MoreKeysThanBatchSize_CreatesMultipleBatches()
        {
            // batchSize=2，5 个键 -> 3 个批次（2+2+1）
            var keys = new List<RedisKey>();
            for (var i = 0; i < 5; i++)
            {
                keys.Add(TraceDefinitions.SortedSetKey(TraceDefinitions.ChannelTraceObject, $"ch-{i}"));
            }

            var serverMock = CreateServerMock(keys.ToArray());
            var dbMock = new Mock<IDatabase>();
            dbMock.SetupGet(d => d.Database).Returns(0);
            var batchMock = CreateCleanupBatchMock(removedPerKey: 1);
            dbMock.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(batchMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider(), writeConcurrency: 20, batchSize: 2);

            var total = await service.RemoveExpiredTraceIndexMembersAsync(serverMock.Object);

            Assert.Equal(5, total);
            dbMock.Verify(d => d.CreateBatch(It.IsAny<object?>()), Times.Exactly(3));
            batchMock.Verify(b => b.Execute(), Times.Exactly(3));
            batchMock.Verify(b => b.SortedSetRemoveRangeByScoreAsync(
                It.IsAny<RedisKey>(), double.NegativeInfinity, ExpectedDefaultCutoffMs,
                Exclude.Stop, CommandFlags.None), Times.Exactly(5));
        }

        #endregion
    }
}
