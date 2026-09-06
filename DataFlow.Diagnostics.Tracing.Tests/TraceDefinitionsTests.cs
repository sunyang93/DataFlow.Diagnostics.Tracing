using DataFlow.Diagnostics.Tracing;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// TraceDefinitions 常量和 Key 构造辅助方法的单元测试。
    /// </summary>
    public class TraceDefinitionsTests
    {
        #region 常量值验证

        [Fact(DisplayName = "ChannelTraceObject 常量值应为 channel")]
        public void ChannelTraceObject_ShouldBe_Channel()
        {
            Assert.Equal("channel", TraceDefinitions.ChannelTraceObject);
        }

        [Fact(DisplayName = "PipelineTraceObject 常量值应为 pipeline")]
        public void PipelineTraceObject_ShouldBe_Pipeline()
        {
            Assert.Equal("pipeline", TraceDefinitions.PipelineTraceObject);
        }

        [Fact(DisplayName = "DefaultRetentionTime 应为 2 小时（毫秒）")]
        public void DefaultRetentionTime_ShouldBe_2Hours_InMilliseconds()
        {
            long expected = 2 * 60 * 60 * 1000;  // 2 小时 = 7,200,000 ms
            Assert.Equal(expected, TraceDefinitions.DefaultRetentionTime);
        }

        [Fact(DisplayName = "JsonRootPath 应为 $（JSON 根路径）")]
        public void JsonRootPath_ShouldBe_DollarSign()
        {
            Assert.Equal("$", TraceDefinitions.JsonRootPath);
        }

        #endregion

        #region Key() 方法

        [Fact(DisplayName = "Key 方法应按 trace:traceObject:objectId:traceItemId 格式拼接")]
        public void Key_ShouldReturn_ExpectedFormat()
        {
            string result = TraceDefinitions.Key("channel", "obj-001", "item-abc");
            Assert.Equal("trace:channel:obj-001:item-abc", result);
        }

        [Theory(DisplayName = "Key 方法 - 不同参数组合返回正确格式")]
        [InlineData("pipeline", "pipe-7", "guid-1", "trace:pipeline:pipe-7:guid-1")]
        [InlineData("channel", "ch-x", "y", "trace:channel:ch-x:y")]
        [InlineData("custom", "", "1", "trace:custom::1")]
        public void Key_WithVariousInputs_ReturnsCorrectFormat(string traceObject, string objectId, string traceItemId, string expected)
        {
            string result = TraceDefinitions.Key(traceObject, objectId, traceItemId);
            Assert.Equal(expected, result);
        }

        #endregion

        #region SortedSetKey() 方法

        [Fact(DisplayName = "SortedSetKey 方法应按 trace:traceObject:objectId:index 格式拼接")]
        public void SortedSetKey_ShouldReturn_ExpectedFormat()
        {
            string result = TraceDefinitions.SortedSetKey("channel", "obj-001");
            Assert.Equal("trace:channel:obj-001:index", result);
        }

        [Theory(DisplayName = "SortedSetKey - 不同参数组合返回正确格式")]
        [InlineData("pipeline", "pipe-7", "trace:pipeline:pipe-7:index")]
        [InlineData("channel", "1", "trace:channel:1:index")]
        public void SortedSetKey_WithVariousInputs_ReturnsCorrectFormat(string traceObject, string objectId, string expected)
        {
            string result = TraceDefinitions.SortedSetKey(traceObject, objectId);
            Assert.Equal(expected, result);
        }

        #endregion

        #region 与具体子类联动的正确性

        [Fact(DisplayName = "ChannelTraceWriteParameter 产生的 Key 应匹配 TraceDefinitions.Key(channel, ...)")]
        public void ChannelTraceWriteParameter_KeyConsistent()
        {
            var param = new ChannelTraceWriteParameter("ch-1", "t-1", "{}");
            string expected = TraceDefinitions.Key(param.TraceObject, param.ObjectId, param.TraceItemId);

            Assert.Equal(TraceDefinitions.ChannelTraceObject, param.TraceObject);
            Assert.Equal("trace:channel:ch-1:t-1", expected);
        }

        [Fact(DisplayName = "PipelineTraceWriteParameter 产生的 Key 应匹配 TraceDefinitions.Key(pipeline, ...)")]
        public void PipelineTraceWriteParameter_KeyConsistent()
        {
            var param = new PipelineTraceWriteParameter("p-2", "t-2", "{}");
            string expected = TraceDefinitions.Key(param.TraceObject, param.ObjectId, param.TraceItemId);

            Assert.Equal(TraceDefinitions.PipelineTraceObject, param.TraceObject);
            Assert.Equal("trace:pipeline:p-2:t-2", expected);
        }

        #endregion
    }
}
