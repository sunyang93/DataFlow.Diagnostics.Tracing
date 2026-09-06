using Xunit;

namespace DataFlow.Diagnostics.Tracing.Tests
{
    /// <summary>
    /// TraceWriteOptions 写入选项类的单元测试。
    /// 重点验证默认值。
    /// </summary>
    public class TraceWriteOptionsTests
    {
        [Fact(DisplayName = "TraceWriteOptions - 默认值：RetentionTimeMilliseconds=null，FireAndForget=false")]
        public void TraceWriteOptions_DefaultValues_AsExpected()
        {
            // Act
            var options = new TraceWriteOptions();

            // Assert
            Assert.Null(options.RetentionTimeMilliseconds);
            Assert.False(options.FireAndForget);
        }

        [Fact(DisplayName = "TraceWriteOptions - 自定义值可正确读写")]
        public void TraceWriteOptions_CustomValues_CanBeReadBack()
        {
            // Act
            var options = new TraceWriteOptions
            {
                RetentionTimeMilliseconds = 60000,
                FireAndForget = true,
            };

            // Assert
            Assert.Equal(60000, options.RetentionTimeMilliseconds);
            Assert.True(options.FireAndForget);
        }

        [Fact(DisplayName = "TraceWriteOptions - RetentionTimeMilliseconds=0 不抛异常（由 TraceService 校验）")]
        public void TraceWriteOptions_ZeroRetention_AllowedInOptions()
        {
            // 选项类本身不校验，校验在 TraceService 内部进行
            var options = new TraceWriteOptions { RetentionTimeMilliseconds = 0 };
            Assert.Equal(0, options.RetentionTimeMilliseconds);
        }
    }
}
