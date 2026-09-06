using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// 基于 Redis（RedisJSON + SortedSet）实现的分布式链路追踪服务。
    /// 存储设计：
    ///   - 文档存储：使用 RedisJSON 以 key=trace:{traceObject}:{objectId}:{traceItemId} 存储 JSON 内容。
    ///   - 时间索引：使用 SortedSet 以 key=trace:{traceObject}:{objectId}:index 存储（文档Key, 时间戳）索引，
    ///     支持按时间范围分页查询。
    /// 写入策略：
    ///   - 单条写入：使用 Redis 事务保证 JSON 和 SortedSet 索引的原子性。
    ///   - 批量写入：使用 Batch 管道 + 限流并发（SemaphoreSlim）+ 分批（batchSize）策略，兼顾吞吐和资源占用。
    /// 所有 key 均设置过期时间，索引过期时间随写入滑动续期，避免索引残留死数据。
    /// </summary>
    public class TraceService : ITraceService
    {
        /// <summary>Redis 数据库实例，用于执行 JSON 写入、SortedSet 操作和事务/批处理。</summary>
        private readonly IDatabase _database;

        /// <summary>时间提供者，用于获取当前 UTC 时间戳；支持测试场景替换为可控时间源。</summary>
        private readonly TimeProvider _timeProvider;

        /// <summary>批量写入时允许的最大并发批次数量，默认 20。</summary>
        private readonly int _writeConcurrency;

        /// <summary>每个批次容纳的 Trace 条目数量，默认 40。</summary>
        private readonly int _batchSize;

        /// <summary>
        /// 初始化 TraceService 实例。
        /// </summary>
        /// <param name="database">Redis 数据库实例，不能为空。</param>
        /// <param name="timeProvider">时间提供者，为 null 时使用系统默认时间源。</param>
        /// <param name="writeConcurrency">批量写入最大并发数，必须大于 0，默认 20。</param>
        /// <param name="batchSize">单批次大小，必须大于 0，默认 40。</param>
        /// <exception cref="ArgumentNullException">当 database 为 null 时抛出。</exception>
        /// <exception cref="ArgumentOutOfRangeException">当 writeConcurrency 或 batchSize 小于等于 0 时抛出。</exception>
        public TraceService(IDatabase database, TimeProvider? timeProvider = null, int writeConcurrency = 20, int batchSize = 40)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _timeProvider = timeProvider ?? System.TimeProvider.System;
            if (writeConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(writeConcurrency));
            if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
            _writeConcurrency = writeConcurrency;
            _batchSize = batchSize;
        }

        /// <inheritdoc />
        public Task WriteChannelTraceAsync(ChannelTraceWriteParameter trace, TraceWriteOptions? options = null)
        {
            if (trace == null) throw new ArgumentNullException(nameof(trace));
            return WriteTraceAsync(trace, options);
        }

        /// <inheritdoc />
        public Task WritePipelineTraceAsync(PipelineTraceWriteParameter trace, TraceWriteOptions? options = null)
        {
            if (trace == null) throw new ArgumentNullException(nameof(trace));
            return WriteTraceAsync(trace, options);
        }

        /// <inheritdoc />
        public Task WriteChannelTracesAsync(IEnumerable<ChannelTraceWriteParameter> traces, TraceWriteOptions? options = null)
        {
            if (traces == null) throw new ArgumentNullException(nameof(traces));
            return WriteTracesAsync(traces.Cast<TraceWriteParameter>(), options);
        }

        /// <inheritdoc />
        public Task WritePipelineTracesAsync(IEnumerable<PipelineTraceWriteParameter> traces, TraceWriteOptions? options = null)
        {
            if (traces == null) throw new ArgumentNullException(nameof(traces));
            return WriteTracesAsync(traces.Cast<TraceWriteParameter>(), options);
        }

        /// <summary>
        /// 批量写入 Trace 数据的内部入口：解析选项 → 分批次执行 → 根据 FireAndForget 决定是否等待。
        /// </summary>
        /// <param name="traces">Trace 写入参数集合。</param>
        /// <param name="options">写入选项，为 null 时使用默认值。</param>
        /// <returns>异步任务。FireAndForget 模式下始终返回已完成的任务。</returns>
        private Task WriteTracesAsync(IEnumerable<TraceWriteParameter> traces, TraceWriteOptions? options = null)
        {
            if (traces == null) throw new ArgumentNullException(nameof(traces));
            var optionsResolved = options ?? new TraceWriteOptions();
            var task = WriteTracesBatchedAsync(traces, optionsResolved);
            return optionsResolved.FireAndForget ? ObserveFireAndForget(task) : task;
        }

        /// <summary>
        /// 单条写入 Trace 数据的内部入口：参数校验 → 核心写入 → 根据 FireAndForget 决定是否等待。
        /// </summary>
        /// <param name="trace">单条 Trace 写入参数。</param>
        /// <param name="options">写入选项，为 null 时使用默认值。</param>
        /// <returns>异步任务。FireAndForget 模式下始终返回已完成的任务。</returns>
        private Task WriteTraceAsync(TraceWriteParameter trace, TraceWriteOptions? options = null)
        {
            if (trace == null) throw new ArgumentNullException(nameof(trace));
            ValidateKeyArguments(trace);
            var optionsResolved = options ?? new TraceWriteOptions();
            var task = WriteTraceCoreAsync(trace, optionsResolved);
            return optionsResolved.FireAndForget ? ObserveFireAndForget(task) : task;
        }

        /// <summary>
        /// 单条事务写入核心逻辑：使用 Redis 事务保证 JSON 文档、SortedSet 索引、两者过期时间四项操作的原子性。
        /// 适用于对单条写入原子性有要求的场景。
        /// </summary>
        /// <param name="trace">单条 Trace 写入参数。</param>
        /// <param name="options">已解析的写入选项。</param>
        /// <exception cref="ArgumentOutOfRangeException">当 RetentionTimeMilliseconds 小于等于 0 时抛出。</exception>
        /// <exception cref="RedisException">当事务提交失败时抛出。</exception>
        private async Task WriteTraceCoreAsync(TraceWriteParameter trace, TraceWriteOptions options)
        {
            // 当前时间戳（毫秒），用作 SortedSet 的 score，即时间索引
            var timestamp = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            // 过期时间：使用自定义或默认值（默认 2 小时）
            var expiration = options.RetentionTimeMilliseconds ?? TraceDefinitions.DefaultRetentionTime;
            if (expiration <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options), "RetentionTimeMilliseconds 必须大于 0。");
            }

            // 构造文档 key 和索引 key
            var documentKey = TraceDefinitions.Key(trace.TraceObject, trace.ObjectId, trace.TraceItemId);
            var indexKey = TraceDefinitions.SortedSetKey(trace.TraceObject, trace.ObjectId);
            // 开启 Redis 事务，保证以下 4 个操作要么全部成功，要么全部失败
            var transaction = _database.CreateTransaction();
            // 1) 使用 RedisJSON 写入文档
            var jsonSetTask = transaction.ExecuteAsync("JSON.SET", documentKey, TraceDefinitions.JsonRootPath, trace.Value);
            // 2) 向 SortedSet 索引添加文档 key，score 为时间戳
            var sortedSetAddTask = transaction.SortedSetAddAsync(indexKey, documentKey, timestamp);
            // 3) 设置文档 key 的过期时间
            var keyExpireTask = transaction.KeyExpireAsync(documentKey, TimeSpan.FromMilliseconds(expiration));
            // 4) 索引键随写入滑动续期，避免文档过期后索引永久残留死 member
            var indexExpireTask = transaction.KeyExpireAsync(indexKey, TimeSpan.FromMilliseconds(expiration));

            // 提交事务，如果返回 false 表示事务条件未满足或执行失败
            if (!await transaction.ExecuteAsync().ConfigureAwait(false))
            {
                throw new RedisException("写入 Trace JSON 和 Sorted Set 索引的事务未提交。");
            }

            // 等待所有内部任务完成，确保异常能正常传播
            await Task.WhenAll(jsonSetTask, sortedSetAddTask, keyExpireTask, indexExpireTask).ConfigureAwait(false);
        }

        /// <summary>
        /// 批量管道写入核心逻辑：按 batchSize 分批次，每批使用 Redis Batch（管道）打包发送，
        /// 同时使用 SemaphoreSlim 限制并发批次数量，避免瞬间占满连接池或服务端资源。
        /// </summary>
        /// <param name="traces">Trace 写入参数集合。</param>
        /// <param name="options">已解析的写入选项。</param>
        private async Task WriteTracesBatchedAsync(IEnumerable<TraceWriteParameter> traces, TraceWriteOptions options)
        {
            if (traces == null) throw new ArgumentNullException(nameof(traces));

            // 限流信号量：控制同时处理的批次数
            var semaphore = new SemaphoreSlim(_writeConcurrency, _writeConcurrency);
            var batchTasks = new List<Task>();

            var batch = new List<TraceWriteParameter>(_batchSize);
            foreach (var trace in traces)
            {
                // 跳过空元素，避免脏数据
                if (trace == null) continue;
                batch.Add(trace);
                // 批次满：拷贝数组后释放，然后启动异步处理
                if (batch.Count >= _batchSize)
                {
                    var captured = batch.ToArray();
                    batch = new List<TraceWriteParameter>(_batchSize);
                    await semaphore.WaitAsync().ConfigureAwait(false);
                    batchTasks.Add(ProcessBatchWithThrottleAsync(captured, options, semaphore));
                }
            }

            // 处理最后一个不足 batchSize 的批次
            if (batch.Count > 0)
            {
                var captured = batch.ToArray();
                await semaphore.WaitAsync().ConfigureAwait(false);
                batchTasks.Add(ProcessBatchWithThrottleAsync(captured, options, semaphore));
            }

            // 等待所有批次完成，任何批次异常都会在此处聚合抛出
            await Task.WhenAll(batchTasks).ConfigureAwait(false);
        }

        /// <summary>
        /// 包装批量处理：在 finally 中释放信号量，保证即使批次异常也不会泄漏信号量；
        /// 同时不吞掉 ProcessBatchAsync 的异常，使批次失败能传播到外层 Task.WhenAll。
        /// </summary>
        /// <param name="batch">单批次 Trace 数组。</param>
        /// <param name="options">写入选项。</param>
        /// <param name="semaphore">限流信号量。</param>
        private async Task ProcessBatchWithThrottleAsync(TraceWriteParameter[] batch, TraceWriteOptions options, SemaphoreSlim semaphore)
        {
            try
            {
                await ProcessBatchAsync(batch, options).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Release();
            }
        }

        /// <summary>
        /// 单批次的实际处理逻辑：使用 Redis Batch（管道模式）将一批操作打包发送，
        /// 与单条事务路径相比，牺牲单条原子性换取更高吞吐量。
        /// </summary>
        /// <param name="batch">单批次 Trace 数组。</param>
        /// <param name="options">写入选项。</param>
        /// <exception cref="ArgumentOutOfRangeException">当 RetentionTimeMilliseconds 小于等于 0 时抛出。</exception>
        private async Task ProcessBatchAsync(TraceWriteParameter[] batch, TraceWriteOptions options)
        {
            if (batch == null) throw new ArgumentNullException(nameof(batch));

            // 创建 Redis Batch（管道）：所有操作会排队，直到 Execute() 时一次性发送
            var b = _database.CreateBatch();
            var tasks = new List<Task>();

            foreach (var trace in batch)
            {
                ValidateKeyArguments(trace);
                var timestamp = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
                var expiration = options.RetentionTimeMilliseconds ?? TraceDefinitions.DefaultRetentionTime;
                if (expiration <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(options), "RetentionTimeMilliseconds 必须大于 0。");
                }

                var documentKey = TraceDefinitions.Key(trace.TraceObject, trace.ObjectId, trace.TraceItemId);
                var indexKey = TraceDefinitions.SortedSetKey(trace.TraceObject, trace.ObjectId);

                // 将操作加入批处理队列，同时跟踪任务以便后续等待完成和观察异常
                tasks.Add(b.ExecuteAsync("JSON.SET", documentKey, TraceDefinitions.JsonRootPath, trace.Value));
                tasks.Add(b.SortedSetAddAsync(indexKey, documentKey, timestamp));
                tasks.Add(b.KeyExpireAsync(documentKey, TimeSpan.FromMilliseconds(expiration)));
                // 索引键随写入滑动续期，与单条事务路径保持一致
                tasks.Add(b.KeyExpireAsync(indexKey, TimeSpan.FromMilliseconds(expiration)));
            }

            // 一次性执行所有排队的操作
            b.Execute();
            // 等待该批次所有操作完成
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task<TraceRangeQueryResult> QueryChannelTraceRangeAsync(ChannelRangeTracesQueryParameter parameter)
        {
            if (parameter == null) throw new ArgumentNullException(nameof(parameter));
            var sortedSetKey = TraceDefinitions.SortedSetKey(parameter.TraceObject, parameter.ObjectId);
            return QueryTraceRangeAsync(parameter, sortedSetKey);
        }

        /// <inheritdoc />
        public Task<TraceRangeQueryResult> QueryPipelineTraceRangeAsync(PipelineRangeTracesQueryParameter parameter)
        {
            if (parameter == null) throw new ArgumentNullException(nameof(parameter));
            var sortedSetKey = TraceDefinitions.SortedSetKey(parameter.TraceObject, parameter.ObjectId);
            return QueryTraceRangeAsync(parameter, sortedSetKey);
        }

        /// <summary>
        /// 范围查询的内部通用实现：使用 Batch 一次性发送计数和分页数据查询，
        /// 保证总数和当前页的口径一致（均使用双侧包含边界 Exclude.None，降序排列）。
        /// </summary>
        /// <param name="parameter">范围查询参数。</param>
        /// <param name="sortedSetKey">SortedSet 索引 key。</param>
        /// <returns>包含总数和当前页 Trace 列表的查询结果。</returns>
        /// <exception cref="ArgumentOutOfRangeException">当 PageIndex 或 PageSize 不合法时抛出。</exception>
        private async Task<TraceRangeQueryResult> QueryTraceRangeAsync(RangeTracesQueryParameter parameter, string sortedSetKey)
        {
            if (parameter == null) throw new ArgumentNullException(nameof(parameter));
            // EndTime 为空时默认为当前时间
            parameter.EndTime = parameter.EndTime ?? _timeProvider.GetUtcNow();
            if (parameter.PageIndex <= 0) throw new ArgumentOutOfRangeException(nameof(parameter.PageIndex), "PageIndex 必须大于 0。");
            if (parameter.PageSize <= 0) throw new ArgumentOutOfRangeException(nameof(parameter.PageSize), "PageSize 必须大于 0。");

            var batch = _database.CreateBatch();
            // 时间范围转换为 Unix 毫秒
            var startMs = parameter.StartTime.ToUnixTimeMilliseconds();
            var endMs = parameter.EndTime.Value.ToUnixTimeMilliseconds();
            // 分页偏移和数量
            var skip = (parameter.PageIndex - 1) * parameter.PageSize;
            var take = parameter.PageSize;

            // 1) 计数：查询该时间范围内 SortedSet 成员总数
            var countTask = batch.SortedSetLengthAsync(sortedSetKey, startMs, endMs);
            // 2) 数据：与计数口径保持一致，双侧边界均包含(Exclude.None)，按时间降序，跳过 skip 条取 take 条
            var dataTask = batch.SortedSetRangeByScoreWithScoresAsync(sortedSetKey, startMs, endMs, Exclude.None, Order.Descending, skip, take);
            batch.Execute();
            await Task.WhenAll(countTask, dataTask).ConfigureAwait(false);
            return new TraceRangeQueryResult(countTask.Result, dataTask.Result);
        }

        /// <inheritdoc />
        public async Task<TraceDocument?> QueryChannelTraceDocumentAsync(string channelId, string traceItemId)
        {
            if (string.IsNullOrWhiteSpace(channelId)) throw new ArgumentException("channelId 不能为空。", nameof(channelId));
            if (string.IsNullOrWhiteSpace(traceItemId)) throw new ArgumentException("traceItemId 不能为空。", nameof(traceItemId));
            var documentKey = TraceDefinitions.Key(TraceDefinitions.ChannelTraceObject, channelId, traceItemId);
            var json = await QueryTraceAsync(documentKey).ConfigureAwait(false);
            return json is null ? null : new TraceDocument(documentKey, json);
        }

        /// <inheritdoc />
        public async Task<TraceDocument?> QueryPipelineTraceDocumentAsync(string pipelineId, string traceItemId)
        {
            if (string.IsNullOrWhiteSpace(pipelineId)) throw new ArgumentException("pipelineId 不能为空。", nameof(pipelineId));
            if (string.IsNullOrWhiteSpace(traceItemId)) throw new ArgumentException("traceItemId 不能为空。", nameof(traceItemId));
            var documentKey = TraceDefinitions.Key(TraceDefinitions.PipelineTraceObject, pipelineId, traceItemId);
            var json = await QueryTraceAsync(documentKey).ConfigureAwait(false);
            return json is null ? null : new TraceDocument(documentKey, json);
        }

        /// <inheritdoc />
        public async Task<string?> QueryTraceAsync(string documentKey)
        {
            ValidateQueryArguments(documentKey);
            // 不带 path 参数：返回裸 JSON 文档；带 "$" 路径会返回数组包裹的 [{...}]
            var result = await _database.ExecuteAsync("JSON.GET", documentKey).ConfigureAwait(false);
            return result.IsNull ? null : result.ToString();
        }

        /// <summary>
        /// 校验文档查询参数。
        /// </summary>
        /// <param name="documentKey">Trace 文档 Key。</param>
        /// <exception cref="ArgumentException">当 documentKey 为空或空白时抛出。</exception>
        private static void ValidateQueryArguments(string documentKey)
        {
            if (string.IsNullOrWhiteSpace(documentKey))
            {
                throw new ArgumentException("DocumentKey 不能为空。", nameof(documentKey));
            }
        }

        /// <summary>
        /// 校验 Trace 写入参数的关键字段。
        /// </summary>
        /// <param name="trace">Trace 写入参数。</param>
        /// <exception cref="ArgumentException">当 TraceObject、ObjectId、TraceItemId 或 Value 为空时抛出。</exception>
        private static void ValidateKeyArguments(TraceWriteParameter trace)
        {
            if (string.IsNullOrWhiteSpace(trace.TraceObject))
            {
                throw new ArgumentException("TraceObject 不能为空。", nameof(TraceWriteParameter.TraceObject));
            }
            if (string.IsNullOrWhiteSpace(trace.ObjectId))
            {
                throw new ArgumentException("ObjectId 不能为空。", nameof(TraceWriteParameter.ObjectId));
            }
            if (string.IsNullOrWhiteSpace(trace.TraceItemId))
            {
                throw new ArgumentException("TraceItemId 不能为空。", nameof(TraceWriteParameter.TraceItemId));
            }
            if (string.IsNullOrWhiteSpace(trace.Value))
            {
                throw new ArgumentException("JSON 数据不能为空。", nameof(TraceWriteParameter.Value));
            }
        }

        /// <summary>
        /// 观察 Fire-and-Forget 模式下的任务：仅捕获异常（避免成为未观察异常），不对外抛出，
        /// 并立即返回已完成任务，使调用方无需等待实际写入完成。
        /// </summary>
        /// <param name="task">需要火并忘记的写入任务。</param>
        /// <returns>始终返回 Task.CompletedTask。</returns>
        private static Task ObserveFireAndForget(Task task)
        {
            task.ContinueWith(
                completedTask => _ = completedTask.Exception,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
            return Task.CompletedTask;
        }
    }
}
