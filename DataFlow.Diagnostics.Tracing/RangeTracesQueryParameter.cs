using System;

namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// 按时间范围分页查询 Trace 的参数基类。
    /// 基于追踪对象类型、业务对象ID和时间区间进行条件过滤，支持分页。
    /// </summary>
    public class RangeTracesQueryParameter
    {
        /// <summary>
        /// 追踪对象类型标识。
        /// 如 "channel" 或 "pipeline"，用于构造 SortedSet 索引 Key。
        /// </summary>
        public string TraceObject { get; }

        /// <summary>
        /// 业务对象唯一ID。
        /// 如通道ID、管道ID，与 TraceObject 组合定位到具体的 SortedSet 索引。
        /// </summary>
        public string ObjectId { get; }

        /// <summary>
        /// 查询时间范围的开始时间（含边界）。
        /// </summary>
        public DateTimeOffset StartTime { get; }

        /// <summary>
        /// 查询时间范围的结束时间（含边界）。
        /// 可空；传入 null 时在查询逻辑中自动填充为当前 UTC 时间。
        /// </summary>
        public DateTimeOffset? EndTime { get; set; }

        /// <summary>
        /// 页码，从 1 开始。
        /// </summary>
        public int PageIndex { get; }

        /// <summary>
        /// 每页大小（每页记录数）。
        /// </summary>
        public int PageSize { get; }

        /// <summary>
        /// 初始化范围查询参数。
        /// </summary>
        /// <param name="traceObject">追踪对象类型标识。</param>
        /// <param name="objectId">业务对象唯一ID。</param>
        /// <param name="startTime">查询开始时间。</param>
        /// <param name="endTime">查询结束时间（可为 null，表示以当前时间为准）。</param>
        /// <param name="pageindex">页码，必须大于 0，默认 1。</param>
        /// <param name="pagesize">页大小，必须大于 0，默认 10。</param>
        /// <exception cref="ArgumentNullException">当 traceObject 或 objectId 为 null 时抛出。</exception>
        /// <exception cref="ArgumentException">当 startTime 大于等于 endTime 时抛出。</exception>
        /// <exception cref="ArgumentOutOfRangeException">当 pageindex 或 pagesize 小于等于 0 时抛出。</exception>
        public RangeTracesQueryParameter(string traceObject, string objectId, DateTimeOffset startTime, DateTimeOffset? endTime, int pageindex = 1, int pagesize = 10)
        {
            TraceObject = traceObject ?? throw new ArgumentNullException(nameof(traceObject));
            ObjectId = objectId ?? throw new ArgumentNullException(nameof(objectId));
            // 若 endTime 有值，必须确保 startTime < endTime
            if (startTime >= endTime)
            {
                throw new ArgumentException("开始时间必须小于截止时间.", nameof(startTime));
            }
            StartTime = startTime;
            EndTime = endTime;
            if (pageindex <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(pageindex), "页码必须大于0.");
            }
            PageIndex = pageindex;
            if (pagesize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(pagesize), "页大小必须大于0.");
            }
            PageSize = pagesize;
        }
    }

    /// <summary>
    /// 通道（Channel）类型的范围查询参数。
    /// 自动将 TraceObject 设置为 "channel"，简化调用方使用。
    /// </summary>
    public class ChannelRangeTracesQueryParameter : RangeTracesQueryParameter
    {
        /// <summary>
        /// 初始化通道范围查询参数。
        /// </summary>
        /// <param name="objectId">通道唯一ID。</param>
        /// <param name="startTime">查询开始时间。</param>
        /// <param name="endTime">查询结束时间（可为 null，表示以当前时间为准）。</param>
        /// <param name="pageindex">页码，必须大于 0，默认 1。</param>
        /// <param name="pagesize">页大小，必须大于 0，默认 10。</param>
        public ChannelRangeTracesQueryParameter(string objectId, DateTimeOffset startTime, DateTimeOffset? endTime, int pageindex = 1, int pagesize = 10)
            : base(TraceDefinitions.ChannelTraceObject, objectId, startTime, endTime, pageindex, pagesize)
        {
        }
    }

    /// <summary>
    /// 管道（Pipeline）类型的范围查询参数。
    /// 自动将 TraceObject 设置为 "pipeline"，简化调用方使用。
    /// </summary>
    public class PipelineRangeTracesQueryParameter : RangeTracesQueryParameter
    {
        /// <summary>
        /// 初始化管道范围查询参数。
        /// </summary>
        /// <param name="objectId">管道唯一ID。</param>
        /// <param name="startTime">查询开始时间。</param>
        /// <param name="endTime">查询结束时间（可为 null，表示以当前时间为准）。</param>
        /// <param name="pageindex">页码，必须大于 0，默认 1。</param>
        /// <param name="pagesize">页大小，必须大于 0，默认 10。</param>
        public PipelineRangeTracesQueryParameter(string objectId, DateTimeOffset startTime, DateTimeOffset? endTime, int pageindex = 1, int pagesize = 10)
            : base(TraceDefinitions.PipelineTraceObject, objectId, startTime, endTime, pageindex, pagesize)
        {
        }
    }
}
