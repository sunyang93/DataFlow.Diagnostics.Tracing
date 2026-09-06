namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// Trace 模块的常量定义和 Key 构造辅助方法。
    /// 统一管理 Redis Key 命名规范、默认保留时间和追踪对象类型常量。
    /// </summary>
    public static class TraceDefinitions
    {
        #region TraceObject

        /// <summary>
        /// 通道（Channel）类型的追踪对象标识。
        /// 用于区分不同业务实体的 Trace 数据，作为 Redis Key 的组成部分。
        /// </summary>
        public const string ChannelTraceObject = "channel";

        /// <summary>
        /// 管道（Pipeline）类型的追踪对象标识。
        /// 用于区分不同业务实体的 Trace 数据，作为 Redis Key 的组成部分。
        /// </summary>
        public const string PipelineTraceObject = "pipeline";

        #endregion

        /// <summary>
        /// 默认的 Trace 数据保留时间，单位：毫秒。
        /// 默认值：2 小时 = 2 * 60 * 60 * 1000 = 7,200,000 ms。
        /// 可通过 TraceWriteOptions.RetentionTimeMilliseconds 覆盖。
        /// </summary>
        public const long DefaultRetentionTime = 2 * 60 * 60 * 1000;

        /// <summary>
        /// RedisJSON 操作使用的根路径。
        /// "$" 表示 JSON 文档的根节点，即对整个文档进行 SET/GET 操作。
        /// </summary>
        public const string JsonRootPath = "$";

        /// <summary>
        /// 构造 Trace 文档存储的 Redis Key。
        /// 格式：trace:{traceObject}:{objectId}:{traceItemId}
        /// 示例：trace:channel:channel-1:550e8400-e29b-41d4-a716-446655440000
        /// </summary>
        /// <param name="traceObject">追踪对象类型，如 "channel" 或 "pipeline"。</param>
        /// <param name="objectId">业务对象ID，如通道ID或管道ID。</param>
        /// <param name="traceItemId">单条追踪项的唯一ID（建议使用 Guid）。</param>
        /// <returns>拼接完成的 Redis 文档 Key。</returns>
        public static string Key(string traceObject, string objectId, string traceItemId)
        {
            return $"trace:{traceObject}:{objectId}:{traceItemId}";
        }

        /// <summary>
        /// 构造 Trace 时间索引（SortedSet）的 Redis Key。
        /// 格式：trace:{traceObject}:{objectId}:index
        /// 示例：trace:channel:channel-1:index
        /// SortedSet 中 member=文档Key，score=Unix时间戳(毫秒)，支持按时间范围分页。
        /// </summary>
        /// <param name="traceObject">追踪对象类型，如 "channel" 或 "pipeline"。</param>
        /// <param name="objectId">业务对象ID，如通道ID或管道ID。</param>
        /// <returns>拼接完成的 Redis SortedSet 索引 Key。</returns>
        public static string SortedSetKey(string traceObject, string objectId)
        {
            return $"trace:{traceObject}:{objectId}:index";
        }
    }
}
