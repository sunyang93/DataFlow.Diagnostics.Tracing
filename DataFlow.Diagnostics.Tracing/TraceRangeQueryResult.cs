using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// 按时间范围分页查询 Trace 的结果对象。
    /// 包含满足条件的总记录数以及当前页的 Trace 索引条目列表。
    /// </summary>
    public class TraceRangeQueryResult
    {
        /// <summary>
        /// 指定时间范围内满足条件的 Trace 总记录数（不考虑分页）。
        /// </summary>
        public long TotalCount { get; }

        /// <summary>
        /// 当前页的 Trace 索引条目列表（只读）。
        /// 每个条目包含文档 Redis Key 和对应的追踪时间戳。
        /// </summary>
        public IReadOnlyList<Trace> Traces { get; }

        /// <summary>
        /// 初始化 TraceRangeQueryResult 实例。
        /// 从 Redis SortedSet 返回的原始条目中提取信息，物化为 Trace 数组。
        /// </summary>
        /// <param name="totalCount">满足条件的总记录数。</param>
        /// <param name="traces">Redis SortedSet 原始条目（member=文档Key, score=时间戳毫秒）。</param>
        public TraceRangeQueryResult(long totalCount, SortedSetEntry[] traces)
        {
            TotalCount = totalCount;
            // 物化为数组：避免每次枚举重复执行 Select 投影
            Traces = traces.Select(x =>
            {
                // 将 Unix 毫秒时间戳转换为 DateTimeOffset
                var traceTime = DateTimeOffset.FromUnixTimeMilliseconds((long)x.Score);
                return new Trace(x.Element.HasValue ? x.Element.ToString() : string.Empty, traceTime);
            }).ToArray();
        }
    }

    /// <summary>
    /// 单条 Trace 的索引条目（范围查询返回的轻量级对象）。
    /// 仅包含文档 Key 和追踪时间，不包含实际的 JSON 内容。
    /// 如需详情，需通过 ITraceService.QueryTraceAsync(documentKey) 再次查询。
    /// </summary>
    public class Trace
    {
        /// <summary>
        /// Trace 文档在 Redis 中存储的 JSON Key。
        /// 格式：trace:{traceObject}:{objectId}:{traceItemId}
        /// </summary>
        public string TraceDocumentJsonKey { get; }

        /// <summary>
        /// 追踪事件发生的时间（UTC，带偏移量）。
        /// 对应 SortedSet 中的 score 值（Unix 毫秒时间戳）。
        /// </summary>
        public DateTimeOffset TraceTime { get; }

        /// <summary>
        /// 初始化 Trace 索引条目。
        /// </summary>
        /// <param name="traceDocumentJsonKey">Trace 文档 Redis Key。</param>
        /// <param name="traceTime">追踪事件时间。</param>
        /// <exception cref="ArgumentNullException">当 traceDocumentJsonKey 为 null 时抛出。</exception>
        public Trace(string traceDocumentJsonKey, DateTimeOffset traceTime)
        {
            TraceDocumentJsonKey = traceDocumentJsonKey ?? throw new ArgumentNullException(nameof(traceDocumentJsonKey));
            TraceTime = traceTime;
        }
    }

    /// <summary>
    /// 单条 Trace 的完整文档对象（文档查询返回）。
    /// 包含文档 Key 和完整的 JSON 内容字符串。
    /// </summary>
    public class TraceDocument
    {
        /// <summary>
        /// Trace 文档在 Redis 中存储的 JSON Key。
        /// 格式：trace:{traceObject}:{objectId}:{traceItemId}
        /// </summary>
        public string TraceDocumentJsonKey { get; }

        /// <summary>
        /// Trace 文档的 JSON 内容字符串（原始格式）。
        /// 调用方需根据业务定义自行反序列化为具体类型。
        /// </summary>
        public string TraceDocumentJson { get; }

        /// <summary>
        /// 初始化 TraceDocument 实例。
        /// </summary>
        /// <param name="traceDocumentJsonKey">Trace 文档 Redis Key。</param>
        /// <param name="traceDocumentJson">Trace 文档的 JSON 内容。</param>
        public TraceDocument(string traceDocumentJsonKey, string traceDocumentJson)
        {
            TraceDocumentJsonKey = traceDocumentJsonKey;
            TraceDocumentJson = traceDocumentJson;
        }
    }
}
