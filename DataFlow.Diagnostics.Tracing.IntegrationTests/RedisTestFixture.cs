using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.IntegrationTests
{
    /// <summary>
    /// Redis 可用性探测器：在测试发现阶段同步连接一次本地 Redis 并缓存结果。
    /// 用于 <see cref="RedisFactAttribute"/> 判断是否跳过整个测试用例。
    /// 连接串由环境变量 TRACE_REDIS_CONNECTION 覆盖，默认 localhost:6379。
    /// </summary>
    internal static class RedisAvailability
    {
        public static readonly string ConnectionString =
            Environment.GetEnvironmentVariable("TRACE_REDIS_CONNECTION")
            ?? "localhost:6379,connectTimeout=2000,syncTimeout=2000,abortConnect=false";

        private static readonly Lazy<(bool Available, string? Reason)> _state = new(Evaluate);

        public static bool IsAvailable => _state.Value.Available;
        public static string? UnavailableReason => _state.Value.Reason;

        /// <summary>
        /// 是否保留测试数据（不清理写入的 Key）。
        /// 设置环境变量 TRACE_REDIS_KEEP_DATA=1 时为 true，用于调试时观察 Redis 中的实际数据。
        /// </summary>
        public static bool KeepTestData =>
            Environment.GetEnvironmentVariable("TRACE_REDIS_KEEP_DATA") == "1";

        private static (bool, string?) Evaluate()
        {
            try
            {
                using var mux = ConnectionMultiplexer.Connect(ConnectionString);
                mux.GetDatabase().Ping();
                return (true, null);
            }
            catch (Exception ex)
            {
                return (false, ex.Message);
            }
        }
    }

    /// <summary>
    /// Redis 集成测试 Fact 特性：当本地 Redis 不可用时自动将测试标记为 Skipped。
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class RedisFactAttribute : FactAttribute
    {
        public RedisFactAttribute()
        {
            if (!RedisAvailability.IsAvailable)
                Skip = $"Redis 不可用，跳过集成测试：{RedisAvailability.UnavailableReason}";
        }
    }

    /// <summary>
    /// Redis 集成测试共享夹具。
    /// 复用单个 <see cref="ConnectionMultiplexer"/> 连接本地 Redis；
    /// 连接失败时置 <see cref="IsAvailable"/>=false（通常已由 <see cref="RedisFactAttribute"/> 在发现阶段跳过）。
    /// </summary>
    public sealed class RedisTestFixture : IAsyncLifetime, IDisposable
    {
        private ConnectionMultiplexer? _multiplexer;

        /// <summary>Redis 是否可用（连通且能 PING）。</summary>
        public bool IsAvailable { get; private set; }

        /// <summary>不可用原因（异常消息）。</summary>
        public string? UnavailableReason { get; private set; }

        /// <summary>获取 Redis 数据库实例（db 0）。</summary>
        public IDatabase GetDatabase() => _multiplexer!.GetDatabase();

        /// <summary>获取 Redis 服务端实例，用于 Keys 扫描等管理操作。</summary>
        public IServer GetServer()
        {
            var endpoints = _multiplexer!.GetEndPoints();
            if (endpoints.Length == 0) throw new InvalidOperationException("Redis 没有可用端点。");
            return _multiplexer.GetServer(endpoints[0]);
        }

        public async Task InitializeAsync()
        {
            try
            {
                _multiplexer = await ConnectionMultiplexer.ConnectAsync(RedisAvailability.ConnectionString).ConfigureAwait(false);
                await _multiplexer.GetDatabase().PingAsync().ConfigureAwait(false);
                IsAvailable = true;
            }
            catch (Exception ex)
            {
                IsAvailable = false;
                UnavailableReason = ex.Message;
            }
        }

        /// <summary>
        /// 按模式扫描并删除匹配的 Key，用于测试间资源隔离清理。
        /// 当环境变量 TRACE_REDIS_KEEP_DATA=1 时跳过清理，保留测试数据供观察。
        /// </summary>
        public async Task CleanupKeysAsync(string pattern)
        {
            if (RedisAvailability.KeepTestData) return;
            if (!IsAvailable || _multiplexer == null) return;
            var keys = new List<RedisKey>();
            await foreach (var key in GetServer().KeysAsync(pattern: pattern).ConfigureAwait(false))
            {
                keys.Add(key);
            }
            if (keys.Count > 0)
            {
                await GetDatabase().KeyDeleteAsync(keys.ToArray()).ConfigureAwait(false);
            }
        }

        public Task DisposeAsync() => Task.CompletedTask;

        public void Dispose()
        {
            if (_multiplexer != null)
            {
                _multiplexer.CloseAsync().GetAwaiter().GetResult();
                _multiplexer.Dispose();
                _multiplexer = null;
            }
        }
    }

    /// <summary>
    /// 集成测试集合定义：禁用并行，避免多测试同时操作同一 Redis 实例造成 Key 冲突。
    /// </summary>
    [CollectionDefinition("RedisIntegration", DisableParallelization = true)]
    public sealed class RedisIntegrationCollection : ICollectionFixture<RedisTestFixture> { }
}
