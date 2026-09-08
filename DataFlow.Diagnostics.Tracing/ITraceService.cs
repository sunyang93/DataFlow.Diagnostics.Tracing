using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// 分布式链路追踪服务接口，提供基于 Redis 的 Trace 写入和查询能力。
    /// 支持 Channel（通道）和 Pipeline（管道）两种追踪对象类型。
    /// </summary>
    public interface ITraceService
    {
        /// <summary>
        /// 异步写入单条通道 Trace 数据。
        /// </summary>
        /// <param name="trace">通道 Trace 写入参数，包含通道ID、追踪项ID和JSON内容。</param>
        /// <param name="options">写入选项，可自定义保留时间和是否火并忘记模式。</param>
        /// <returns>表示异步写入操作的任务。</returns>
        /// <exception cref="System.ArgumentNullException">当 trace 为 null 时抛出。</exception>
        Task WriteChannelTraceAsync(ChannelTraceWriteParameter trace, TraceWriteOptions? options = null);

        /// <summary>
        /// 异步写入单条管道 Trace 数据。
        /// </summary>
        /// <param name="trace">管道 Trace 写入参数，包含管道ID、追踪项ID和JSON内容。</param>
        /// <param name="options">写入选项，可自定义保留时间和是否火并忘记模式。</param>
        /// <returns>表示异步写入操作的任务。</returns>
        /// <exception cref="System.ArgumentNullException">当 trace 为 null 时抛出。</exception>
        Task WritePipelineTraceAsync(PipelineTraceWriteParameter trace, TraceWriteOptions? options = null);

        /// <summary>
        /// 异步批量写入通道 Trace 数据。
        /// 采用分批+限流并发策略，适合大批量高并发写入场景。
        /// </summary>
        /// <param name="traces">通道 Trace 写入参数集合。</param>
        /// <param name="options">写入选项，可自定义保留时间和是否火并忘记模式。</param>
        /// <returns>表示异步批量写入操作的任务。</returns>
        /// <exception cref="System.ArgumentNullException">当 traces 为 null 时抛出。</exception>
        Task WriteChannelTracesAsync(IEnumerable<ChannelTraceWriteParameter> traces, TraceWriteOptions? options = null);

        /// <summary>
        /// 异步批量写入管道 Trace 数据。
        /// 采用分批+限流并发策略，适合大批量高并发写入场景。
        /// </summary>
        /// <param name="traces">管道 Trace 写入参数集合。</param>
        /// <param name="options">写入选项，可自定义保留时间和是否火并忘记模式。</param>
        /// <returns>表示异步批量写入操作的任务。</returns>
        /// <exception cref="System.ArgumentNullException">当 traces 为 null 时抛出。</exception>
        Task WritePipelineTracesAsync(IEnumerable<PipelineTraceWriteParameter> traces, TraceWriteOptions? options = null);

        /// <summary>
        /// 按时间范围分页查询通道 Trace 列表（索引查询，不含详细JSON内容）。
        /// 返回结果包含总记录数和当前页的 Trace 索引条目（文档Key+时间戳）。
        /// </summary>
        /// <param name="parameter">通道范围查询参数，包含通道ID、时间范围、分页信息。</param>
        /// <returns>包含总数和当前页 Trace 列表的查询结果。</returns>
        /// <exception cref="System.ArgumentNullException">当 parameter 为 null 时抛出。</exception>
        /// <exception cref="System.ArgumentOutOfRangeException">当 PageIndex 或 PageSize 不合法时抛出。</exception>
        Task<TraceRangeQueryResult> QueryChannelTraceRangeAsync(ChannelRangeTracesQueryParameter parameter);

        /// <summary>
        /// 按时间范围分页查询管道 Trace 列表（索引查询，不含详细JSON内容）。
        /// 返回结果包含总记录数和当前页的 Trace 索引条目（文档Key+时间戳）。
        /// </summary>
        /// <param name="parameter">管道范围查询参数，包含管道ID、时间范围、分页信息。</param>
        /// <returns>包含总数和当前页 Trace 列表的查询结果。</returns>
        /// <exception cref="System.ArgumentNullException">当 parameter 为 null 时抛出。</exception>
        /// <exception cref="System.ArgumentOutOfRangeException">当 PageIndex 或 PageSize 不合法时抛出。</exception>
        Task<TraceRangeQueryResult> QueryPipelineTraceRangeAsync(PipelineRangeTracesQueryParameter parameter);

        /// <summary>
        /// 查询单条通道 Trace 的完整 JSON 文档。
        /// </summary>
        /// <param name="channelId">通道ID。</param>
        /// <param name="traceItemId">追踪项ID。</param>
        /// <returns>Trace 文档对象（包含文档Key和JSON内容），未找到时返回 null。</returns>
        /// <exception cref="System.ArgumentException">当 channelId 或 traceItemId 为空时抛出。</exception>
        Task<TraceDocument?> QueryChannelTraceDocumentAsync(string channelId, string traceItemId);

        /// <summary>
        /// 查询单条管道 Trace 的完整 JSON 文档。
        /// </summary>
        /// <param name="pipelineId">管道ID。</param>
        /// <param name="traceItemId">追踪项ID。</param>
        /// <returns>Trace 文档对象（包含文档Key和JSON内容），未找到时返回 null。</returns>
        /// <exception cref="System.ArgumentException">当 pipelineId 或 traceItemId 为空时抛出。</exception>
        Task<TraceDocument?> QueryPipelineTraceDocumentAsync(string pipelineId, string traceItemId);

        /// <summary>
        /// 按文档 Key 直接查询 Trace 的 JSON 内容。
        /// 适用于已通过范围查询拿到文档 Key 后，按需获取详情的场景。
        /// </summary>
        /// <param name="documentKey">Trace 文档 Redis Key（格式：trace:{traceObject}:{objectId}:{traceItemId}）。</param>
        /// <returns>JSON 字符串，未找到时返回 null。</returns>
        /// <exception cref="System.ArgumentException">当 documentKey 为空时抛出。</exception>
        Task<string?> QueryTraceAsync(string documentKey);

        /// <summary>
        /// 清理所有时间索引 SortedSet 中已过期的 member。
        /// 通过 SCAN 扫描全部 trace:*:index 索引键，再使用 Batch（管道）批量执行 ZREMRANGEBYSCORE，
        /// 仅移除 score（写入时间戳）早于“当前时间 - 保留期”的 member；JSON 文档本身由 TTL 自动过期，本方法不删除文档。
        /// 适用于定时维护任务：在持续写入导致索引键滑动续期时，清除指向已过期文档的残留 member。
        /// </summary>
        /// <param name="server">Redis 服务端实例，用于 SCAN 扫描索引键（可由 ConnectionMultiplexer.GetServer 获取）。</param>
        /// <param name="retentionMilliseconds">保留期（毫秒），为 null 时使用默认值 2 小时；score 早于 now-retention 的 member 会被移除。</param>
        /// <returns>所有索引键中被移除的 member 总数。</returns>
        /// <exception cref="System.ArgumentNullException">当 server 为 null 时抛出。</exception>
        /// <exception cref="System.ArgumentOutOfRangeException">当 retentionMilliseconds 小于等于 0 时抛出。</exception>
        Task<long> RemoveExpiredTraceIndexMembersAsync(IServer server, long? retentionMilliseconds = null);
    }
}
