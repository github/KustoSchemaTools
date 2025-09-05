using KustoSchemaTools.Parser;
using Xunit;

namespace KustoSchemaTools.Tests.Parser
{
    public class KustoExtensionsTests
    {
        [Theory]
        [InlineData("https://trd-abc.kusto.fabric.microsoft.com", false, "https://trd-abc.kusto.fabric.microsoft.com")]
        [InlineData("https://trd-abc.kusto.fabric.microsoft.com", true,  "https://trd-abc.kusto.fabric.microsoft.com")]
        [InlineData("trd-abc.kusto.fabric.microsoft.com", false, "https://trd-abc.kusto.fabric.microsoft.com")]
        [InlineData("trd-abc.kusto.fabric.microsoft.com", true,  "https://ingest-trd-abc.kusto.fabric.microsoft.com")]
        [InlineData("myadx.eastus", false, "https://myadx.eastus.kusto.windows.net")]
        [InlineData("myadx.eastus", true,  "https://ingest-myadx.eastus.kusto.windows.net")]
        public void Fqdn_AndShorthand_MapToExpectedHosts(string input, bool ingest, string expected)
        {
            var actual = KustoExtensions.ToKustoClusterUrl(input, ingest);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void NullOrEmpty_ReturnsEmpty_NotThrow()
        {
            var a = KustoExtensions.ToKustoClusterUrl(null!, false);
            var b = KustoExtensions.ToKustoClusterUrl("", true);
            Assert.Equal(string.Empty, a);
            Assert.Equal(string.Empty, b);
        }
    }
}
