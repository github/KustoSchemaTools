using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class ContinuousExportModelTests
    {
        [Fact]
        public void ContinuousExport_Should_Initialize_With_Default_Values()
        {
            // Act
            var continuousExport = new ContinuousExport();

            // Assert
            continuousExport.ExternalTable.Should().BeNull();
            continuousExport.Query.Should().BeNull();
            continuousExport.ManagedIdentity.Should().BeNull();
            continuousExport.IntervalBetweenRuns.Should().Be(0);
            continuousExport.ForcedLatencyInMinutes.Should().Be(0);
            continuousExport.SizeLimit.Should().Be(0);
            continuousExport.Distributed.Should().BeFalse();
        }

        [Fact]
        public void ContinuousExport_Should_Allow_Property_Assignment()
        {
            // Arrange
            var continuousExport = new ContinuousExport();

            // Act
            continuousExport.ExternalTable = "ExternalEvents";
            continuousExport.Query = "Events | where timestamp > ago(1h)";
            continuousExport.ManagedIdentity = "system";
            continuousExport.IntervalBetweenRuns = 30;
            continuousExport.ForcedLatencyInMinutes = 5;
            continuousExport.SizeLimit = 1000000;
            continuousExport.Distributed = true;

            // Assert
            continuousExport.ExternalTable.Should().Be("ExternalEvents");
            continuousExport.Query.Should().Be("Events | where timestamp > ago(1h)");
            continuousExport.ManagedIdentity.Should().Be("system");
            continuousExport.IntervalBetweenRuns.Should().Be(30);
            continuousExport.ForcedLatencyInMinutes.Should().Be(5);
            continuousExport.SizeLimit.Should().Be(1000000);
            continuousExport.Distributed.Should().BeTrue();
        }

        [Fact]
        public void ContinuousExport_Should_Generate_Creation_Script()
        {
            // Arrange
            var continuousExport = new ContinuousExport
            {
                ExternalTable = "ExternalEvents",
                Query = "Events | where timestamp > ago(1h)",
                ManagedIdentity = "system",
                IntervalBetweenRuns = 30,
                ForcedLatencyInMinutes = 5
            };

            // Act
            var scripts = continuousExport.CreateScripts("HourlyEvents", true);

            // Assert
            scripts.Should().NotBeEmpty();
            var script = scripts.First();
            script.Kind.Should().Be("ContinuousExport");
            script.Text.Should().Contain(".create-or-alter continuous-export HourlyEvents");
            script.Text.Should().Contain("to table ExternalEvents");
            script.Text.Should().Contain("Events | where timestamp > ago(1h)");
            script.Text.Should().Contain("intervalBetweenRuns=5m");
            script.Text.Should().Contain("managedIdentity='system'");
        }
    }
}
