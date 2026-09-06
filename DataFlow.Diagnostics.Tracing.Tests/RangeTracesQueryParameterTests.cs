using DataFlow.Diagnostics.Tracing;
using System;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// RangeTracesQueryParameter 及其子类构造函数的单元测试。
    /// 覆盖时间范围校验、分页校验、TraceObject 自动注入。
    /// </summary>
    public class RangeTracesQueryParameterTests
    {
        #region 基类 - 正常构造

        [Fact(DisplayName = "RangeTracesQueryParameter - 正常参数正确赋值")]
        public void RangeTracesQueryParameter_Constructor_AssignsProperties()
        {
            // Arrange
            var startTime = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var endTime = startTime.AddHours(1);

            // Act
            var p = new RangeTracesQueryParameter("channel", "o-1", startTime, endTime, 2, 50);

            // Assert
            Assert.Equal("channel", p.TraceObject);
            Assert.Equal("o-1", p.ObjectId);
            Assert.Equal(startTime, p.StartTime);
            Assert.Equal(endTime, p.EndTime);
            Assert.Equal(2, p.PageIndex);
            Assert.Equal(50, p.PageSize);
        }

        [Fact(DisplayName = "RangeTracesQueryParameter - endTime 为 null 时不抛异常（合法）")]
        public void RangeTracesQueryParameter_Constructor_NullEndTime_Allowed()
        {
            // Arrange
            var startTime = DateTimeOffset.UtcNow;

            // Act (不抛异常)
            var p = new RangeTracesQueryParameter("channel", "o-1", startTime, null);

            // Assert
            Assert.Null(p.EndTime);
        }

        [Fact(DisplayName = "RangeTracesQueryParameter - 未传 pageindex/pagesize 时使用默认值 1 和 10")]
        public void RangeTracesQueryParameter_Constructor_DefaultPagingValues()
        {
            // Arrange
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddMinutes(1);

            // Act
            var p = new RangeTracesQueryParameter("channel", "o-1", startTime, endTime);

            // Assert
            Assert.Equal(1, p.PageIndex);
            Assert.Equal(10, p.PageSize);
        }

        #endregion

        #region 基类 - 参数校验异常

        [Fact(DisplayName = "RangeTracesQueryParameter - traceObject 为 null 抛出 ArgumentNullException")]
        public void RangeTracesQueryParameter_NullTraceObject_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new RangeTracesQueryParameter(null!, "o-1", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddHours(1)));
            Assert.Equal("traceObject", ex.ParamName);
        }

        [Fact(DisplayName = "RangeTracesQueryParameter - objectId 为 null 抛出 ArgumentNullException")]
        public void RangeTracesQueryParameter_NullObjectId_Throws()
        {
            var ex = Assert.Throws<ArgumentNullException>(() =>
                new RangeTracesQueryParameter("channel", null!, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddHours(1)));
            Assert.Equal("objectId", ex.ParamName);
        }

        [Fact(DisplayName = "RangeTracesQueryParameter - startTime >= endTime 抛出 ArgumentException")]
        public void RangeTracesQueryParameter_StartTimeAfterEndTime_Throws()
        {
            // Arrange
            var startTime = new DateTimeOffset(2025, 1, 2, 0, 0, 0, TimeSpan.Zero);
            var endTime = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);  // end < start

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() =>
                new RangeTracesQueryParameter("channel", "o-1", startTime, endTime));
            Assert.Equal("startTime", ex.ParamName);
            Assert.Contains("开始时间必须小于截止时间", ex.Message);
        }

        [Fact(DisplayName = "RangeTracesQueryParameter - startTime == endTime 抛出 ArgumentException")]
        public void RangeTracesQueryParameter_StartEqualsEnd_Throws()
        {
            // Arrange
            var t = DateTimeOffset.UtcNow;

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() =>
                new RangeTracesQueryParameter("channel", "o-1", t, t));
            Assert.Equal("startTime", ex.ParamName);
        }

        [Theory(DisplayName = "RangeTracesQueryParameter - pageindex <= 0 抛出 ArgumentOutOfRangeException")]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(int.MinValue)]
        public void RangeTracesQueryParameter_InvalidPageIndex_Throws(int pageindex)
        {
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddMinutes(1);

            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new RangeTracesQueryParameter("channel", "o-1", startTime, endTime, pageindex));
            Assert.Equal("pageindex", ex.ParamName);
            Assert.Contains("页码必须大于0", ex.Message);
        }

        [Theory(DisplayName = "RangeTracesQueryParameter - pagesize <= 0 抛出 ArgumentOutOfRangeException")]
        [InlineData(0)]
        [InlineData(-5)]
        public void RangeTracesQueryParameter_InvalidPageSize_Throws(int pagesize)
        {
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddMinutes(1);

            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new RangeTracesQueryParameter("channel", "o-1", startTime, endTime, 1, pagesize));
            Assert.Equal("pagesize", ex.ParamName);
            Assert.Contains("页大小必须大于0", ex.Message);
        }

        #endregion

        #region 子类 - TraceObject 自动注入

        [Fact(DisplayName = "ChannelRangeTracesQueryParameter - TraceObject 自动设为 channel")]
        public void ChannelRangeTracesQueryParameter_SetsTraceObject_ToChannel()
        {
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddMinutes(1);

            var p = new ChannelRangeTracesQueryParameter("ch-1", startTime, endTime, 1, 20);

            Assert.Equal(TraceDefinitions.ChannelTraceObject, p.TraceObject);
            Assert.Equal("ch-1", p.ObjectId);
        }

        [Fact(DisplayName = "PipelineRangeTracesQueryParameter - TraceObject 自动设为 pipeline")]
        public void PipelineRangeTracesQueryParameter_SetsTraceObject_ToPipeline()
        {
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddMinutes(1);

            var p = new PipelineRangeTracesQueryParameter("p-1", startTime, endTime);

            Assert.Equal(TraceDefinitions.PipelineTraceObject, p.TraceObject);
            Assert.Equal("p-1", p.ObjectId);
        }

        [Fact(DisplayName = "ChannelRangeTracesQueryParameter - 校验异常会正常抛出（通过基类）")]
        public void ChannelRangeTracesQueryParameter_InvalidPageIndex_Throws()
        {
            var startTime = DateTimeOffset.UtcNow;
            var endTime = startTime.AddMinutes(1);

            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new ChannelRangeTracesQueryParameter("ch-1", startTime, endTime, 0));
            Assert.Equal("pageindex", ex.ParamName);
        }

        #endregion
    }
}
