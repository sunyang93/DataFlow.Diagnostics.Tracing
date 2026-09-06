namespace DataFlow.Diagnostics.Tracing
{
    /// <summary>
    /// Trace 写入选项，用于控制单次/批量 Trace 写入操作的行为。
    /// </summary>
    public sealed class TraceWriteOptions
    {
        /// <summary>
        /// 自定义 Trace 数据保留时间（毫秒）。
        /// 为 null 时使用默认值 <see cref="TraceDefinitions.DefaultRetentionTime"/>（2 小时）。
        /// 该值同时作用于文档 Key 和 SortedSet 索引 Key 的过期时间。
        /// </summary>
        public long? RetentionTimeMilliseconds { get; set; }

        /// <summary>
        /// 是否启用 Fire-and-Forget（火并忘记）写入模式。
        ///   - true：写入方法立即返回已完成任务，不等待 Redis 实际应答；
        ///           异常仅被内部观察，不会抛出给调用方。适用于高频且不关心结果的写入路径。
        ///   - false：默认值，写入方法会等待 Redis 应答，异常会正常向外传播。
        /// 默认 false。
        /// </summary>
        public bool FireAndForget { get; set; }
    }
}
