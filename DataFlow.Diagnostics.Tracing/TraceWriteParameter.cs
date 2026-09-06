using System;

namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// Trace 写入参数基类。
    /// 描述一条追踪记录的元数据（追踪对象类型、业务ID、追踪项ID）和 JSON 内容。
    /// </summary>
    public class TraceWriteParameter
    {
        /// <summary>
        /// 初始化 TraceWriteParameter 实例。
        /// </summary>
        /// <param name="traceObject">追踪对象类型标识，如 "channel" 或 "pipeline"。</param>
        /// <param name="objectId">业务对象唯一ID，如通道ID或管道ID。</param>
        /// <param name="traceItemId">单条追踪项的唯一ID（建议使用 Guid）。</param>
        /// <param name="value">追踪数据的 JSON 字符串内容。</param>
        /// <exception cref="ArgumentNullException">当任一参数为 null 时抛出。</exception>
        public TraceWriteParameter(string traceObject, string objectId, string traceItemId, string value)
        {
            TraceObject = traceObject ?? throw new ArgumentNullException(nameof(traceObject));
            ObjectId = objectId ?? throw new ArgumentNullException(nameof(objectId));
            TraceItemId = traceItemId ?? throw new ArgumentNullException(nameof(traceItemId));
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// 追踪对象类型标识。
        /// 用于区分不同业务实体（如 channel、pipeline），将参与 Redis Key 构造。
        /// </summary>
        public string TraceObject { get; }

        /// <summary>
        /// 业务对象唯一ID。
        /// 如通道ID、管道ID，与 TraceObject 组合定位到具体的业务实体。
        /// </summary>
        public string ObjectId { get; }

        /// <summary>
        /// 单条追踪项的唯一ID（建议使用 Guid）。
        /// 用于在同一业务对象下区分不同的追踪记录。
        /// </summary>
        public string TraceItemId { get; }

        /// <summary>
        /// 追踪数据的 JSON 字符串内容。
        /// 由调用方根据业务需要自行构造和序列化。
        /// </summary>
        public string Value { get; }
    }

    /// <summary>
    /// 通道（Channel）类型的 Trace 写入参数。
    /// 自动将 TraceObject 设置为 "channel"，简化调用方使用。
    /// </summary>
    public class ChannelTraceWriteParameter : TraceWriteParameter
    {
        /// <summary>
        /// 初始化通道 Trace 写入参数。
        /// </summary>
        /// <param name="channelId">通道唯一ID。</param>
        /// <param name="traceItemId">单条追踪项的唯一ID（建议使用 Guid）。</param>
        /// <param name="value">追踪数据的 JSON 字符串内容。</param>
        public ChannelTraceWriteParameter(string channelId, string traceItemId, string value)
            : base(TraceDefinitions.ChannelTraceObject, channelId, traceItemId, value)
        {
        }
    }

    /// <summary>
    /// 管道（Pipeline）类型的 Trace 写入参数。
    /// 自动将 TraceObject 设置为 "pipeline"，简化调用方使用。
    /// </summary>
    public class PipelineTraceWriteParameter : TraceWriteParameter
    {
        /// <summary>
        /// 初始化管道 Trace 写入参数。
        /// </summary>
        /// <param name="pipelineId">管道唯一ID。</param>
        /// <param name="traceItemId">单条追踪项的唯一ID（建议使用 Guid）。</param>
        /// <param name="value">追踪数据的 JSON 字符串内容。</param>
        public PipelineTraceWriteParameter(string pipelineId, string traceItemId, string value)
            : base(TraceDefinitions.PipelineTraceObject, pipelineId, traceItemId, value)
        {
        }
    }
}
