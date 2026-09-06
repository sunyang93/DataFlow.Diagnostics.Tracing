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
    /// TraceService 的单元测试（写入路径 + 构造函数 + 参数校验）。
    /// 使用 Moq 模拟 StackExchange.Redis 的 IDatabase / ITransaction / IBatch 接口，
    /// 使用可控的 TimeProvider 提供稳定时间源，避免依赖真实 Redis。
    /// </summary>
    public partial class TraceServiceTests
    {
        // 统一的固定 UTC 时间戳，所有测试共用以保证可重复性
        private static readonly DateTimeOffset FixedUtcNow = new(2025, 6, 1, 12, 0, 0, TimeSpan.Zero);
        private const long FixedUtcNowMs = 1748779200000; // FixedUtcNow.ToUnixTimeMilliseconds()

        /// <summary>创建固定返回 FixedUtcNow 的 TimeProvider mock。</summary>
        private static TimeProvider CreateFixedTimeProvider()
        {
            var mockTp = new Mock<TimeProvider>();
            mockTp.Setup(tp => tp.GetUtcNow()).Returns(FixedUtcNow);
            return mockTp.Object;
        }

        /// <summary>构造返回 "OK" 的 RedisResult（模拟 JSON.SET 成功）。</summary>
        private static RedisResult OkResult() => RedisResult.Create((RedisValue)"OK");

        /// <summary>构造返回指定 JSON 字符串的 RedisResult（模拟 JSON.GET）。</summary>
        private static RedisResult JsonResult(string json) => RedisResult.Create((RedisValue)json);

        /// <summary>构造空 RedisResult（模拟 JSON.GET 未命中，IsNull=true）。</summary>
        private static RedisResult NullResult() => RedisResult.Create(RedisValue.Null);

        private static Mock<IDatabase> CreateEmptyDatabaseMock() => new();

        #region 构造函数参数校验

        [Fact(DisplayName = "TraceService 构造函数 - database 为 null 抛出 ArgumentNullException")]
        public void Constructor_NullDatabase_ThrowsArgumentNullException()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new TraceService(null!));
            Assert.Equal("database", ex.ParamName);
        }

        [Theory(DisplayName = "TraceService 构造函数 - writeConcurrency <= 0 抛出 ArgumentOutOfRangeException")]
        [InlineData(0)]
        [InlineData(-1)]
        public void Constructor_InvalidWriteConcurrency_Throws(int writeConcurrency)
        {
            var db = CreateEmptyDatabaseMock().Object;
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new TraceService(db, writeConcurrency: writeConcurrency));
            Assert.Equal("writeConcurrency", ex.ParamName);
        }

        [Theory(DisplayName = "TraceService 构造函数 - batchSize <= 0 抛出 ArgumentOutOfRangeException")]
        [InlineData(0)]
        [InlineData(-10)]
        public void Constructor_InvalidBatchSize_Throws(int batchSize)
        {
            var db = CreateEmptyDatabaseMock().Object;
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new TraceService(db, batchSize: batchSize));
            Assert.Equal("batchSize", ex.ParamName);
        }

        [Fact(DisplayName = "TraceService 构造函数 - timeProvider 为 null 时使用默认系统时间源，不抛异常")]
        public void Constructor_NullTimeProvider_UsesSystemTime()
        {
            var db = CreateEmptyDatabaseMock().Object;
            var service = new TraceService(db, timeProvider: null);
            Assert.NotNull(service);
        }

        [Fact(DisplayName = "TraceService 构造函数 - 合法参数创建成功且实现 ITraceService")]
        public void Constructor_ValidArguments_CreatesInstance()
        {
            var db = CreateEmptyDatabaseMock().Object;
            var service = new TraceService(db, CreateFixedTimeProvider(), 10, 20);
            Assert.NotNull(service);
            Assert.IsAssignableFrom<ITraceService>(service);
        }

        #endregion

        #region 写入方法 - 参数校验

        [Fact(DisplayName = "WriteChannelTraceAsync - trace 为 null 抛出 ArgumentNullException")]
        public async Task WriteChannelTraceAsync_NullTrace_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => service.WriteChannelTraceAsync(null!));
            Assert.Equal("trace", ex.ParamName);
        }

        [Fact(DisplayName = "WritePipelineTraceAsync - trace 为 null 抛出 ArgumentNullException")]
        public async Task WritePipelineTraceAsync_NullTrace_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => service.WritePipelineTraceAsync(null!));
            Assert.Equal("trace", ex.ParamName);
        }

        [Fact(DisplayName = "WriteChannelTracesAsync - traces 为 null 抛出 ArgumentNullException")]
        public async Task WriteChannelTracesAsync_NullTraces_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => service.WriteChannelTracesAsync(null!));
            Assert.Equal("traces", ex.ParamName);
        }

        [Fact(DisplayName = "WritePipelineTracesAsync - traces 为 null 抛出 ArgumentNullException")]
        public async Task WritePipelineTracesAsync_NullTraces_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => service.WritePipelineTracesAsync(null!));
            Assert.Equal("traces", ex.ParamName);
        }

        [Fact(DisplayName = "批量写入 - trace 项为 null 的元素会被跳过，不影响其余项")]
        public async Task WriteChannelTracesAsync_SkipsNullTraceItems_WithoutThrowing()
        {
            var trace1 = new ChannelTraceWriteParameter("ch-1", Guid.NewGuid().ToString(), "{}");
            var dbMock = new Mock<IDatabase>();
            SetupBatchForWrites(dbMock);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var traces = new List<ChannelTraceWriteParameter?> { null, trace1 };

            // 不抛异常
            await service.WriteChannelTracesAsync(traces!);

            var batchMock = GetLastBatchMock(dbMock);
            batchMock.Verify(b => b.Execute(), Times.Once);
        }

        #endregion

        #region 单条写入核心流程（事务路径）

        [Fact(DisplayName = "WriteChannelTraceAsync - 正常流程：创建事务，JSON.SET、SortedSetAdd、2x KeyExpire 并提交事务")]
        public async Task WriteChannelTraceAsync_Success_TransactionIsExecutedWithExpectedOps()
        {
            string channelId = "ch-1";
            string traceItemId = "item-abc";
            string jsonValue = """{"status":"ok"}""";
            var param = new ChannelTraceWriteParameter(channelId, traceItemId, jsonValue);

            string expectedDocKey = TraceDefinitions.Key("channel", channelId, traceItemId);
            string expectedIndexKey = TraceDefinitions.SortedSetKey("channel", channelId);
            var expectedExpiration = TimeSpan.FromMilliseconds(TraceDefinitions.DefaultRetentionTime);

            var dbMock = new Mock<IDatabase>();
            var transactionMock = new Mock<ITransaction>();
            transactionMock.Setup(t => t.ExecuteAsync(CommandFlags.None)).ReturnsAsync(true);
            // JSON.SET 命令：签名为 ExecuteAsync(string command, params object?[] args)，返回 RedisResult
            transactionMock.Setup(t => t.ExecuteAsync("JSON.SET", expectedDocKey, TraceDefinitions.JsonRootPath, jsonValue))
                .ReturnsAsync(OkResult());
            transactionMock.Setup(t => t.SortedSetAddAsync(expectedIndexKey, expectedDocKey, FixedUtcNowMs, CommandFlags.None))
                .ReturnsAsync(true);
            transactionMock.Setup(t => t.KeyExpireAsync(expectedDocKey, expectedExpiration, CommandFlags.None))
                .ReturnsAsync(true);
            transactionMock.Setup(t => t.KeyExpireAsync(expectedIndexKey, expectedExpiration, CommandFlags.None))
                .ReturnsAsync(true);
            dbMock.Setup(d => d.CreateTransaction(It.IsAny<object?>())).Returns(transactionMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            await service.WriteChannelTraceAsync(param);

            dbMock.Verify(d => d.CreateTransaction(It.IsAny<object?>()), Times.Once);
            transactionMock.Verify(t => t.ExecuteAsync(CommandFlags.None), Times.Once);

            transactionMock.Verify(t => t.ExecuteAsync("JSON.SET", expectedDocKey, TraceDefinitions.JsonRootPath, jsonValue), Times.Once);
            transactionMock.Verify(t => t.SortedSetAddAsync(expectedIndexKey, expectedDocKey, FixedUtcNowMs, SortedSetWhen.Always, CommandFlags.None), Times.Once);
            transactionMock.Verify(t => t.KeyExpireAsync(expectedDocKey, expectedExpiration, ExpireWhen.Always, CommandFlags.None), Times.Once);
            transactionMock.Verify(t => t.KeyExpireAsync(expectedIndexKey, expectedExpiration, ExpireWhen.Always, CommandFlags.None), Times.Once);
        }

        [Fact(DisplayName = "WritePipelineTraceAsync - 自定义保留时间：KeyExpire 参数应匹配自定义值")]
        public async Task WritePipelineTraceAsync_CustomRetention_KeyExpireMatches()
        {
            long customRetentionMs = 5 * 60 * 1000;
            var options = new TraceWriteOptions { RetentionTimeMilliseconds = customRetentionMs };
            var param = new PipelineTraceWriteParameter("p-1", "t-1", "{}");
            var expectedExpiration = TimeSpan.FromMilliseconds(customRetentionMs);

            var dbMock = new Mock<IDatabase>();
            var txMock = new Mock<ITransaction>();
            txMock.Setup(t => t.ExecuteAsync(CommandFlags.None)).ReturnsAsync(true);
            txMock.Setup(t => t.ExecuteAsync(It.IsAny<string>(), It.IsAny<object[]>()))
                .ReturnsAsync(OkResult());
            txMock.Setup(t => t.SortedSetAddAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<double>(), SortedSetWhen.Always, CommandFlags.None))
                .ReturnsAsync(true);
            txMock.Setup(t => t.KeyExpireAsync(It.IsAny<RedisKey>(), expectedExpiration, ExpireWhen.Always, CommandFlags.None))
                .ReturnsAsync(true);
            dbMock.Setup(d => d.CreateTransaction(It.IsAny<object?>())).Returns(txMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            await service.WritePipelineTraceAsync(param, options);

            string expectedDocKey = TraceDefinitions.Key("pipeline", "p-1", "t-1");
            string expectedIndexKey = TraceDefinitions.SortedSetKey("pipeline", "p-1");
            txMock.Verify(t => t.KeyExpireAsync(expectedDocKey, expectedExpiration, ExpireWhen.Always, CommandFlags.None), Times.Once);
            txMock.Verify(t => t.KeyExpireAsync(expectedIndexKey, expectedExpiration, ExpireWhen.Always, CommandFlags.None), Times.Once);
        }

        [Fact(DisplayName = "单条写入 - RetentionTimeMilliseconds<=0 抛出 ArgumentOutOfRangeException（不执行任何 Redis 操作）")]
        public async Task WriteTraceAsync_ZeroRetention_Throws_BeforeRedisCall()
        {
            var options = new TraceWriteOptions { RetentionTimeMilliseconds = 0 };
            var param = new ChannelTraceWriteParameter("ch-1", "t-1", "{}");
            var dbMock = new Mock<IDatabase>(MockBehavior.Strict);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                service.WriteChannelTraceAsync(param, options));
            Assert.Contains("RetentionTimeMilliseconds", ex.Message);
        }

        [Fact(DisplayName = "单条写入 - 事务 Execute 返回 false 抛出 RedisException")]
        public async Task WriteTraceAsync_TransactionFailed_ThrowsRedisException()
        {
            var param = new ChannelTraceWriteParameter("ch-1", "t-1", "{}");
            var dbMock = new Mock<IDatabase>();
            var txMock = new Mock<ITransaction>();
            txMock.Setup(t => t.ExecuteAsync(CommandFlags.None)).ReturnsAsync(false);
            dbMock.Setup(d => d.CreateTransaction(It.IsAny<object?>())).Returns(txMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<RedisException>(() => service.WriteChannelTraceAsync(param));
            Assert.Contains("事务未提交", ex.Message);
        }

        [Fact(DisplayName = "单条写入 FireAndForget 模式：立即返回 CompletedTask（即使 Redis 异常也不向外抛）")]
        public async Task WriteChannelTraceAsync_FireAndForget_ReturnsCompletedTaskImmediately()
        {
            var options = new TraceWriteOptions { FireAndForget = true };
            var param = new ChannelTraceWriteParameter("ch-1", "t-1", "{}");

            var dbMock = new Mock<IDatabase>();
            var txMock = new Mock<ITransaction>();
            txMock.Setup(t => t.ExecuteAsync(CommandFlags.None)).ThrowsAsync(new RedisException("boom"));
            dbMock.Setup(d => d.CreateTransaction(It.IsAny<object?>())).Returns(txMock.Object);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var task = service.WriteChannelTraceAsync(param, options);

            Assert.True(task.IsCompleted, "FireAndForget 模式应立即返回已完成任务。");
            Assert.Equal(TaskStatus.RanToCompletion, task.Status);
            await Task.Delay(10); // 让 ContinueWith 完成，避免污染后续测试
        }

        #endregion

        #region 批量写入（Batch + 限流）

        [Fact(DisplayName = "WriteChannelTracesAsync - 3 条小于 batchSize(40)：1 个 Batch、1 次 Execute、每条 4 个操作")]
        public async Task WriteChannelTracesAsync_SmallBatch_OneBatchExecute()
        {
            var traces = new List<ChannelTraceWriteParameter>
            {
                new("ch-1", "t1", "{}"),
                new("ch-1", "t2", "{}"),
                new("ch-1", "t3", "{}"),
            };

            var dbMock = new Mock<IDatabase>();
            SetupBatchForWrites(dbMock);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            await service.WriteChannelTracesAsync(traces);

            var batchMock = GetLastBatchMock(dbMock);
            batchMock.Verify(b => b.Execute(), Times.Once);
            // JSON.SET 3 次
            batchMock.Verify(b => b.ExecuteAsync("JSON.SET", It.IsAny<object?[]>()), Times.Exactly(3));
            // SortedSetAdd 3 次
            batchMock.Verify(b => b.SortedSetAddAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<double>(), SortedSetWhen.Always, CommandFlags.None), Times.Exactly(3));
            // KeyExpire 6 次（文档 3 + 索引 3）
            batchMock.Verify(b => b.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan>(), ExpireWhen.Always, CommandFlags.None), Times.Exactly(6));
        }

        [Fact(DisplayName = "批量写入 - 自定义 RetentionTime<=0 抛出 ArgumentOutOfRangeException")]
        public async Task WriteTracesAsync_InvalidCustomRetention_Throws()
        {
            var traces = new[] { new ChannelTraceWriteParameter("ch-1", "t1", "{}") };
            var options = new TraceWriteOptions { RetentionTimeMilliseconds = -1 };
            // Loose：CreateBatch 在校验每条记录 retention 之前被调用，需要允许该调用
            var dbMock = new Mock<IDatabase>();
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                service.WriteChannelTracesAsync(traces, options));
            Assert.Contains("RetentionTimeMilliseconds", ex.Message);
        }

        [Fact(DisplayName = "批量写入 - 50 条默认 batchSize=40：分成 2 个批次（40+10），调用 2 次 CreateBatch")]
        public async Task WritePipelineTracesAsync_50Items_DefaultBatchSize_TwoBatches()
        {
            var traces = new List<PipelineTraceWriteParameter>();
            for (int i = 0; i < 50; i++)
                traces.Add(new PipelineTraceWriteParameter("p-1", $"t{i}", $"{{\"i\":{i}}}"));

            var dbMock = new Mock<IDatabase>();
            SetupBatchForWrites(dbMock);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            await service.WritePipelineTracesAsync(traces);

            dbMock.Verify(d => d.CreateBatch(It.IsAny<object?>()), Times.Exactly(2));
        }

        [Fact(DisplayName = "批量写入 FireAndForget 模式：立即返回 CompletedTask，不等待实际 Redis 操作")]
        public async Task WriteChannelTracesAsync_FireAndForget_ReturnsCompletedTaskImmediately()
        {
            var traces = new[] { new ChannelTraceWriteParameter("ch-1", "t1", "{}") };
            var options = new TraceWriteOptions { FireAndForget = true };

            var dbMock = new Mock<IDatabase>();
            var batchMock = new Mock<IBatch>();
            batchMock.Setup(b => b.Execute()).Throws(new RedisException("redis is down"));
            SetupBatchReturns(dbMock, batchMock);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var task = service.WriteChannelTracesAsync(traces, options);

            Assert.True(task.IsCompleted, "FireAndForget 模式应立即返回已完成任务。");
            await Task.Delay(10);
        }

        #endregion
    }

    // ====================== 辅助方法（Moq 配置） ======================
    public partial class TraceServiceTests
    {
        private static Mock<IBatch>? _lastBatchMock;

        private static Mock<IBatch> GetLastBatchMock(Mock<IDatabase> _)
        {
            return _lastBatchMock ?? throw new InvalidOperationException("请先调用 SetupBatchForWrites / SetupBatchReturns。");
        }

        private static void SetupBatchReturns(Mock<IDatabase> dbMock, Mock<IBatch> batchMock)
        {
            _lastBatchMock = batchMock;
            dbMock.Setup(d => d.CreateBatch(It.IsAny<object?>())).Returns(batchMock.Object);
        }

        /// <summary>
        /// 配置 IDatabase.CreateBatch 返回一个"假工作"的 IBatch：
        /// JSON.SET / SortedSetAdd / KeyExpire 均返回成功，Execute() 正常返回。
        /// </summary>
        private static void SetupBatchForWrites(Mock<IDatabase> dbMock)
        {
            var batchMock = new Mock<IBatch>();
            batchMock.Setup(b => b.ExecuteAsync(It.IsAny<string>(), It.IsAny<object[]>()))
                .ReturnsAsync(OkResult());
            batchMock.Setup(b => b.SortedSetAddAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<double>(), SortedSetWhen.Always, CommandFlags.None))
                .ReturnsAsync(true);
            batchMock.Setup(b => b.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan>(), ExpireWhen.Always, CommandFlags.None))
                .ReturnsAsync(true);
            batchMock.Setup(b => b.Execute());

            SetupBatchReturns(dbMock, batchMock);
        }
    }
}
