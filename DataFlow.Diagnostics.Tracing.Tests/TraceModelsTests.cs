using System;
using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// Trace、TraceDocument、TraceRangeQueryResult 模型类的单元测试。
    /// </summary>
    public class TraceModelsTests
    {
        #region Trace 类

        [Fact(DisplayName = "Trace 构造函数 - 参数正确赋值")]
        public void Trace_Constructor_AssignsProperties()
        {
            // Arrange
            string expectedKey = "trace:channel:ch-1:item-1";
            var expectedTime = new DateTimeOffset(2025, 1, 2, 3, 4, 5, TimeSpan.Zero);

            // Act
            var trace = new Trace(expectedKey, expectedTime);

            // Assert
            Assert.Equal(expectedKey, trace.TraceDocumentJsonKey);
            Assert.Equal(expectedTime, trace.TraceTime);
        }

        [Fact(DisplayName = "Trace 构造函数 - traceDocumentJsonKey 为 null 时抛出 ArgumentNullException")]
        public void Trace_Constructor_NullKey_ThrowsArgumentNullException()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => new Trace(null!, DateTimeOffset.UtcNow));
            Assert.Equal("traceDocumentJsonKey", ex.ParamName);
        }

        [Fact(DisplayName = "Trace 构造函数 - 空字符串 Key 合法（允许空值）")]
        public void Trace_Constructor_EmptyKey_IsAllowed()
        {
            // Act
            var trace = new Trace(string.Empty, DateTimeOffset.UtcNow);

            // Assert
            Assert.Equal(string.Empty, trace.TraceDocumentJsonKey);
        }

        #endregion

        #region TraceDocument 类

        [Fact(DisplayName = "TraceDocument 构造函数 - 参数正确赋值")]
        public void TraceDocument_Constructor_AssignsProperties()
        {
            // Arrange
            string expectedKey = "trace:pipeline:p-1:i-1";
            string expectedJson = """{"name":"test","value":123}""";

            // Act
            var doc = new TraceDocument(expectedKey, expectedJson);

            // Assert
            Assert.Equal(expectedKey, doc.TraceDocumentJsonKey);
            Assert.Equal(expectedJson, doc.TraceDocumentJson);
        }

        [Theory(DisplayName = "TraceDocument - null / 空值 允许赋值（不作校验）")]
        [InlineData(null, null)]
        [InlineData("", "")]
        [InlineData(null, "{}")]
        public void TraceDocument_Constructor_AllowsNullAndEmpty(string? key, string? json)
        {
            // Act (不应抛异常)
            var doc = new TraceDocument(key!, json!);

            // Assert
            Assert.Equal(key, doc.TraceDocumentJsonKey);
            Assert.Equal(json, doc.TraceDocumentJson);
        }

        #endregion
    }
}
