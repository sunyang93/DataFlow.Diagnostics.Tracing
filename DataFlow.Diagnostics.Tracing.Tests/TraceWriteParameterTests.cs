using DataFlow.Diagnostics.Tracing;
using System;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// TraceWriteParameter 及其子类（Channel/Pipeline）构造函数的单元测试。
    /// 重点覆盖参数校验和 TraceObject 自动注入。
    /// </summary>
    public class TraceWriteParameterTests
    {
        #region 基类 TraceWriteParameter - 参数校验

        [Fact(DisplayName = "TraceWriteParameter 构造函数 - 正常参数正确赋值")]
        public void TraceWriteParameter_Constructor_AssignsProperties()
        {
            // Arrange
            string traceObject = "channel";
            string objectId = "o-1";
            string traceItemId = "i-1";
            string value = "{}";

            // Act
            var p = new TraceWriteParameter(traceObject, objectId, traceItemId, value);

            // Assert
            Assert.Equal(traceObject, p.TraceObject);
            Assert.Equal(objectId, p.ObjectId);
            Assert.Equal(traceItemId, p.TraceItemId);
            Assert.Equal(value, p.Value);
        }

        [Theory(DisplayName = "TraceWriteParameter 构造函数 - null 参数抛出 ArgumentNullException")]
        [InlineData("traceObject", null, "ok", "ok", "ok")]
        [InlineData("objectId", "ok", null, "ok", "ok")]
        [InlineData("traceItemId", "ok", "ok", null, "ok")]
        [InlineData("value", "ok", "ok", "ok", null)]
        public void TraceWriteParameter_Constructor_NullArgument_ThrowsArgumentNullException(
            string expectedParamName, string? traceObject, string? objectId, string? traceItemId, string? value)
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new TraceWriteParameter(traceObject!, objectId!, traceItemId!, value!));
            Assert.Equal(expectedParamName, ex.ParamName);
        }

        #endregion

        #region ChannelTraceWriteParameter 子类

        [Fact(DisplayName = "ChannelTraceWriteParameter - TraceObject 自动设为 channel")]
        public void ChannelTraceWriteParameter_SetsTraceObject_ToChannel()
        {
            // Act
            var p = new ChannelTraceWriteParameter("ch-1", "t-1", "{}");

            // Assert
            Assert.Equal(TraceDefinitions.ChannelTraceObject, p.TraceObject);
            Assert.Equal("ch-1", p.ObjectId);
            Assert.Equal("t-1", p.TraceItemId);
            Assert.Equal("{}", p.Value);
        }

        [Theory(DisplayName = "ChannelTraceWriteParameter - null 参数抛出 ArgumentNullException")]
        [InlineData(null, "t-1", "{}", "traceObject")]
        // 注意：ChannelTraceWriteParameter 内部传 channelId 作为 base 的 objectId 参数
        // base 构造函数中 objectId=null 会抛 objectId；而此处 channelId 直接传入 objectId 位置
        public void ChannelTraceWriteParameter_NullChannelId_Throws(string? channelId, string traceItemId, string value, string _)
        {
            // channelId 映射为 base 构造的 objectId
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new ChannelTraceWriteParameter(channelId!, traceItemId, value));
            Assert.Equal("objectId", ex.ParamName);
        }

        [Fact(DisplayName = "ChannelTraceWriteParameter - traceItemId 为 null 抛出 ArgumentNullException")]
        public void ChannelTraceWriteParameter_NullTraceItemId_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new ChannelTraceWriteParameter("ch-1", null!, "{}"));
            Assert.Equal("traceItemId", ex.ParamName);
        }

        [Fact(DisplayName = "ChannelTraceWriteParameter - value 为 null 抛出 ArgumentNullException")]
        public void ChannelTraceWriteParameter_NullValue_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new ChannelTraceWriteParameter("ch-1", "t-1", null!));
            Assert.Equal("value", ex.ParamName);
        }

        #endregion

        #region PipelineTraceWriteParameter 子类

        [Fact(DisplayName = "PipelineTraceWriteParameter - TraceObject 自动设为 pipeline")]
        public void PipelineTraceWriteParameter_SetsTraceObject_ToPipeline()
        {
            // Act
            var p = new PipelineTraceWriteParameter("p-1", "t-1", "{}");

            // Assert
            Assert.Equal(TraceDefinitions.PipelineTraceObject, p.TraceObject);
            Assert.Equal("p-1", p.ObjectId);
            Assert.Equal("t-1", p.TraceItemId);
            Assert.Equal("{}", p.Value);
        }

        [Fact(DisplayName = "PipelineTraceWriteParameter - pipelineId 为 null 抛出 ArgumentNullException")]
        public void PipelineTraceWriteParameter_NullPipelineId_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new PipelineTraceWriteParameter(null!, "t-1", "{}"));
            Assert.Equal("objectId", ex.ParamName);
        }

        [Fact(DisplayName = "PipelineTraceWriteParameter - traceItemId 为 null 抛出 ArgumentNullException")]
        public void PipelineTraceWriteParameter_NullTraceItemId_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new PipelineTraceWriteParameter("p-1", null!, "{}"));
            Assert.Equal("traceItemId", ex.ParamName);
        }

        [Fact(DisplayName = "PipelineTraceWriteParameter - value 为 null 抛出 ArgumentNullException")]
        public void PipelineTraceWriteParameter_NullValue_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new PipelineTraceWriteParameter("p-1", "t-1", null!));
            Assert.Equal("value", ex.ParamName);
        }

        #endregion
    }
}
