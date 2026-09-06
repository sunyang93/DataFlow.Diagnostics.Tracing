using DataFlow.Diagnostics.Tracing;
using Moq;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// TraceService 查询相关方法的单元测试（partial 类）。
    /// 范围查询、单文档查询、按 Key 直接查询的参数校验和正常返回逻辑。
    /// </summary>
    public partial class TraceServiceTests
    {
        #region 范围查询 - 参数校验

        [Fact(DisplayName = "QueryChannelTraceRangeAsync - parameter 为 null 抛出 ArgumentNullException")]
        public async Task QueryChannelTraceRangeAsync_NullParameter_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => service.QueryChannelTraceRangeAsync(null!));
            Assert.Equal("parameter", ex.ParamName);
        }

        [Fact(DisplayName = "QueryPipelineTraceRangeAsync - parameter 为 null 抛出 ArgumentNullException")]
        public async Task QueryPipelineTraceRangeAsync_NullParameter_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => service.QueryPipelineTraceRangeAsync(null!));
            Assert.Equal("parameter", ex.ParamName);
        }

        [Fact(DisplayName = "范围查询 - PageIndex=0 抛出 ArgumentOutOfRangeException")]
        public async Task QueryRangeTraceAsync_PageIndexZero_ThrowsArgumentOutOfRange()
        {
            var startTime = FixedUtcNow.AddHours(-1);
            var endTime = FixedUtcNow;
            var dbMock = new Mock<IDatabase>(MockBehavior.Strict); // 严格模式确保不调用 Redis
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            // 注意：ChannelRangeTracesQueryParameter 构造函数本身会校验 pageindex；TraceService.Query... 内也做了二次校验。
            // 把"构造参数+调用服务"全部放入 lambda，保证异常被正确捕获（无论是在哪一层抛出）。
            var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            {
                var param = new ChannelRangeTracesQueryParameter("ch-1", startTime, endTime, pageindex: 0, pagesize: 10);
                return service.QueryChannelTraceRangeAsync(param);
            });

            // 两个位置都可能抛出：构造函数 (paramName=pageindex) 或 TraceService (ParamName=PageIndex)
            Assert.Contains("ageIndex", ex.ParamName, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("大于0", ex.Message);
        }

        [Fact(DisplayName = "范围查询 - PageSize=0 抛出 ArgumentOutOfRangeException")]
        public async Task QueryRangeTraceAsync_PageSizeZero_ThrowsArgumentOutOfRange()
        {
            var startTime = FixedUtcNow.AddHours(-1);
            var endTime = FixedUtcNow;
            var dbMock = new Mock<IDatabase>(MockBehavior.Strict);
            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());

            var ex = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            {
                var param = new PipelineRangeTracesQueryParameter("p-1", startTime, endTime, pageindex: 1, pagesize: 0);
                return service.QueryPipelineTraceRangeAsync(param);
            });

            Assert.Contains("ageSize", ex.ParamName, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("大于0", ex.Message);
        }

        #endregion

        #region 范围查询 - 正常流程

        [Fact(DisplayName = "QueryChannelTraceRangeAsync - EndTime 为 null 时自动使用 TimeProvider 当前时间，第 1 页正确")]
        public async Task QueryChannelTraceRangeAsync_NullEndTime_UsesNowFromTimeProvider()
        {
            var startTime = FixedUtcNow.AddMinutes(-10);
            // EndTime = null → 服务端应使用 FixedUtcNow
            var param = new ChannelRangeTracesQueryParameter("ch-1", startTime, endTime: null, pageindex: 1, pagesize: 2);

            string expectedIndexKey = TraceDefinitions.SortedSetKey("channel", "ch-1");
            long startMs = startTime.ToUnixTimeMilliseconds();
            long endMs = FixedUtcNowMs;
            const long expectedTotal = 3L;
            var data = new[]
            {
                new SortedSetEntry((RedisValue)"trace:channel:ch-1:t3", FixedUtcNowMs),
                new SortedSetEntry((RedisValue)"trace:channel:ch-1:t2", FixedUtcNowMs - 60000),
            };

            var dbMock = new Mock<IDatabase>();
            SetupBatchForRangeQuery(dbMock, expectedIndexKey, startMs, endMs, skip: 0, take: 2, totalCount: expectedTotal, data: data);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var result = await service.QueryChannelTraceRangeAsync(param);

            Assert.Equal(expectedTotal, result.TotalCount);
            Assert.Equal(2, result.Traces.Count);
            Assert.Equal("trace:channel:ch-1:t3", result.Traces[0].TraceDocumentJsonKey);
            Assert.Equal(FixedUtcNow, result.Traces[0].TraceTime);
            Assert.Equal(FixedUtcNow.AddMinutes(-1), result.Traces[1].TraceTime);

            var batchMock = GetLastBatchMock(dbMock);
            batchMock.Verify(b => b.SortedSetLengthAsync(expectedIndexKey, startMs, endMs, Exclude.None, CommandFlags.None), Times.Once);
            batchMock.Verify(b => b.SortedSetRangeByScoreWithScoresAsync(expectedIndexKey, startMs, endMs,
                Exclude.None, Order.Descending, 0, 2, CommandFlags.None), Times.Once);
            batchMock.Verify(b => b.Execute(), Times.Once);
        }

        [Fact(DisplayName = "QueryPipelineTraceRangeAsync - 第 2 页 skip=(2-1)*5=5，参数正确传递")]
        public async Task QueryPipelineTraceRangeAsync_Page2_SkipAndTakeAreCorrect()
        {
            var startTime = FixedUtcNow.AddHours(-2);
            var endTime = FixedUtcNow.AddHours(-1);
            var param = new PipelineRangeTracesQueryParameter("p-9", startTime, endTime, pageindex: 2, pagesize: 5);

            string expectedIndexKey = TraceDefinitions.SortedSetKey("pipeline", "p-9");
            long startMs = startTime.ToUnixTimeMilliseconds();
            long endMs = endTime.ToUnixTimeMilliseconds();

            var dbMock = new Mock<IDatabase>();
            SetupBatchForRangeQuery(dbMock, expectedIndexKey, startMs, endMs, skip: 5, take: 5, totalCount: 12, data: Array.Empty<SortedSetEntry>());

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var result = await service.QueryPipelineTraceRangeAsync(param);

            Assert.Equal(12, result.TotalCount);
            Assert.Empty(result.Traces);

            var batchMock = GetLastBatchMock(dbMock);
            batchMock.Verify(b => b.SortedSetRangeByScoreWithScoresAsync(expectedIndexKey, startMs, endMs,
                Exclude.None, Order.Descending, 5, 5, CommandFlags.None), Times.Once);
        }

        [Fact(DisplayName = "范围查询结果 - SortedSetEntry element 无值时使用空字符串构造 Trace（不抛异常）")]
        public async Task QueryRangeTraceAsync_EmptySortedSetEntryValue_ReturnsEmptyStringKey()
        {
            var startTime = FixedUtcNow.AddMinutes(-5);
            var endTime = FixedUtcNow;
            var param = new ChannelRangeTracesQueryParameter("ch-x", startTime, endTime, 1, 1);

            var data = new[] { new SortedSetEntry(RedisValue.Null, FixedUtcNowMs) };
            var dbMock = new Mock<IDatabase>();
            SetupBatchForRangeQuery(dbMock,
                TraceDefinitions.SortedSetKey("channel", "ch-x"),
                startTime.ToUnixTimeMilliseconds(), endTime.ToUnixTimeMilliseconds(),
                0, 1, totalCount: 1, data: data);

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var result = await service.QueryChannelTraceRangeAsync(param);

            Assert.Single(result.Traces);
            Assert.Equal(string.Empty, result.Traces[0].TraceDocumentJsonKey);
            Assert.Equal(FixedUtcNow, result.Traces[0].TraceTime);
        }

        #endregion

        #region 单文档查询（按 channelId/pipelineId + traceItemId）

        [Theory(DisplayName = "QueryChannelTraceDocumentAsync - channelId 为空抛出 ArgumentException")]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public async Task QueryChannelTraceDocumentAsync_InvalidChannelId_Throws(string? channelId)
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => service.QueryChannelTraceDocumentAsync(channelId!, "t-1"));
            Assert.Equal("channelId", ex.ParamName);
        }

        [Theory(DisplayName = "QueryChannelTraceDocumentAsync - traceItemId 为空抛出 ArgumentException")]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("\t ")]
        public async Task QueryChannelTraceDocumentAsync_InvalidTraceItemId_Throws(string? traceItemId)
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => service.QueryChannelTraceDocumentAsync("ch-1", traceItemId!));
            Assert.Equal("traceItemId", ex.ParamName);
        }

        [Theory(DisplayName = "QueryPipelineTraceDocumentAsync - pipelineId 为空抛出 ArgumentException")]
        [InlineData(null)]
        [InlineData("")]
        public async Task QueryPipelineTraceDocumentAsync_InvalidPipelineId_Throws(string? pipelineId)
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => service.QueryPipelineTraceDocumentAsync(pipelineId!, "t-1"));
            Assert.Equal("pipelineId", ex.ParamName);
        }

        [Fact(DisplayName = "QueryPipelineTraceDocumentAsync - traceItemId 空白抛出 ArgumentException")]
        public async Task QueryPipelineTraceDocumentAsync_InvalidTraceItemId_Throws()
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => service.QueryPipelineTraceDocumentAsync("p-1", ""));
            Assert.Equal("traceItemId", ex.ParamName);
        }

        [Fact(DisplayName = "QueryChannelTraceDocumentAsync - 找到文档：返回非 null TraceDocument，JSON 和 Key 正确")]
        public async Task QueryChannelTraceDocumentAsync_Found_ReturnsDocument()
        {
            string channelId = "ch-42";
            string traceItemId = "item-xyz";
            string expectedJson = """{"channel":"A","status":"RUNNING"}""";
            string expectedKey = TraceDefinitions.Key("channel", channelId, traceItemId);

            var dbMock = new Mock<IDatabase>();
            dbMock.Setup(d => d.ExecuteAsync("JSON.GET", expectedKey))
                .ReturnsAsync(JsonResult(expectedJson));

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var doc = await service.QueryChannelTraceDocumentAsync(channelId, traceItemId);

            Assert.NotNull(doc);
            Assert.Equal(expectedKey, doc!.TraceDocumentJsonKey);
            Assert.Equal(expectedJson, doc.TraceDocumentJson);
        }

        [Fact(DisplayName = "QueryPipelineTraceDocumentAsync - 未找到文档（Redis 返回 null）：返回 null")]
        public async Task QueryPipelineTraceDocumentAsync_NotFound_ReturnsNull()
        {
            var dbMock = new Mock<IDatabase>();
            // 宽松匹配 params object[]（string → 单元素数组）
            dbMock.Setup(d => d.ExecuteAsync("JSON.GET", It.IsAny<object[]>()))
                .ReturnsAsync(NullResult());

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var doc = await service.QueryPipelineTraceDocumentAsync("p-not-exist", "t-1");

            Assert.Null(doc);
        }

        #endregion

        #region QueryTraceAsync - 按文档 Key 直接查询 JSON

        [Theory(DisplayName = "QueryTraceAsync - documentKey 为空抛出 ArgumentException")]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public async Task QueryTraceAsync_InvalidDocumentKey_Throws(string? key)
        {
            var service = new TraceService(CreateEmptyDatabaseMock().Object, CreateFixedTimeProvider());
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => service.QueryTraceAsync(key!));
            Assert.Equal("documentKey", ex.ParamName);
        }

        [Fact(DisplayName = "QueryTraceAsync - 命中：返回 JSON 字符串")]
        public async Task QueryTraceAsync_Hit_ReturnsJsonString()
        {
            string key = "trace:channel:ch-5:t-77";
            string json = """{"v":1}""";
            var dbMock = new Mock<IDatabase>();
            dbMock.Setup(d => d.ExecuteAsync("JSON.GET", key))
                .ReturnsAsync(JsonResult(json));

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var result = await service.QueryTraceAsync(key);

            Assert.Equal(json, result);
        }

        [Fact(DisplayName = "QueryTraceAsync - 未命中：返回 null")]
        public async Task QueryTraceAsync_Miss_ReturnsNull()
        {
            var dbMock = new Mock<IDatabase>();
            // 宽松匹配 params object[]：documentKey(string) → 打包为单个元素 object[]
            dbMock.Setup(d => d.ExecuteAsync("JSON.GET", It.IsAny<object[]>()))
                .ReturnsAsync(NullResult());

            var service = new TraceService(dbMock.Object, CreateFixedTimeProvider());
            var result = await service.QueryTraceAsync("trace:nonexistent");

            Assert.Null(result);
        }

        #endregion

        #region TraceRangeQueryResult 构造 - 时间戳转换正确性

        [Fact(DisplayName = "TraceRangeQueryResult - SortedSetEntry score(ms) 正确转换为 DateTimeOffset")]
        public void TraceRangeQueryResult_Score_ConvertsToDateTimeOffset()
        {
            long ts1Ms = FixedUtcNowMs;
            long ts2Ms = FixedUtcNowMs - 3600_000;  // 1 小时前
            var entries = new[]
            {
                new SortedSetEntry((RedisValue)"k1", ts1Ms),
                new SortedSetEntry((RedisValue)"k2", ts2Ms),
            };

            var result = new TraceRangeQueryResult(totalCount: 2, traces: entries);

            Assert.Equal(2, result.TotalCount);
            Assert.Equal(2, result.Traces.Count);
            Assert.Equal("k1", result.Traces[0].TraceDocumentJsonKey);
            Assert.Equal(FixedUtcNow, result.Traces[0].TraceTime);
            Assert.Equal("k2", result.Traces[1].TraceDocumentJsonKey);
            Assert.Equal(FixedUtcNow.AddHours(-1), result.Traces[1].TraceTime);
        }

        [Fact(DisplayName = "TraceRangeQueryResult - 空数组不抛异常，TotalCount=0, Traces.Count=0")]
        public void TraceRangeQueryResult_EmptyEntries_ZeroCount()
        {
            var result = new TraceRangeQueryResult(0, Array.Empty<SortedSetEntry>());

            Assert.Equal(0, result.TotalCount);
            Assert.Empty(result.Traces);
        }

        [Fact(DisplayName = "TraceRangeQueryResult - Traces 物化为只读数组，避免重复枚举")]
        public void TraceRangeQueryResult_MaterializedToReadOnlyArray()
        {
            var entries = new[] { new SortedSetEntry((RedisValue)"a", FixedUtcNowMs) };
            var result = new TraceRangeQueryResult(1, entries);

            Assert.IsType<Trace[]>(result.Traces);
        }

        #endregion
    }

    // ====================== 查询场景辅助方法 ======================
    public partial class TraceServiceTests
    {
        /// <summary>
        /// 为范围查询场景配置 IDatabase：
        ///   - IDatabase.CreateBatch() 返回配置好的 IBatch mock；
        ///   - IBatch.SortedSetLengthAsync 返回 totalCount；
        ///   - IBatch.SortedSetRangeByScoreWithScoresAsync(..., skip, take) 返回 data；
        ///   - IBatch.Execute() 正常返回。
        /// </summary>
        private static void SetupBatchForRangeQuery(
            Mock<IDatabase> dbMock,
            string expectedSortedSetKey,
            long startMs, long endMs,
            long skip, long take,
            long totalCount,
            SortedSetEntry[] data)
        {
            var batchMock = new Mock<IBatch>();
            batchMock.Setup(b => b.SortedSetLengthAsync(
                    expectedSortedSetKey, startMs, endMs, Exclude.None, CommandFlags.None))
                .ReturnsAsync(totalCount);
            batchMock.Setup(b => b.SortedSetRangeByScoreWithScoresAsync(
                    expectedSortedSetKey, startMs, endMs,
                    Exclude.None, Order.Descending, skip, take, CommandFlags.None))
                .ReturnsAsync(data);
            batchMock.Setup(b => b.Execute());

            SetupBatchReturns(dbMock, batchMock);
        }
    }
}
