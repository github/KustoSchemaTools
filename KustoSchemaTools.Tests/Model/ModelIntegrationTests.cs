using FluentAssertions;
using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Tests.Model
{
    /// <summary>
    /// Comprehensive model tests for KustoSchemaTools models
    /// </summary>
    public class ModelIntegrationTests
    {
        [Fact]
        public void Database_Should_Support_Complex_Configuration()
        {
            // Arrange
            var database = new Database
            {
                Name = "TestDatabase",
                Team = "DataEngineering",
                Admins = new List<AADObject>
                {
                    new AADObject { Name = "admin@company.com", Id = "admin-guid" }
                },
                Tables = new Dictionary<string, Table>
                {
                    ["Events"] = new Table
                    {
                        Folder = "Raw",
                        DocString = "Raw events table",
                        Columns = new Dictionary<string, string>
                        {
                            ["EventId"] = "string",
                            ["Timestamp"] = "datetime",
                            ["Data"] = "dynamic"
                        },
                        Policies = new TablePolicy
                        {
                            Retention = "365d",
                            HotCache = "30d",
                            RestrictedViewAccess = false
                        }
                    }
                },
                Functions = new Dictionary<string, Function>
                {
                    ["GetRecentEvents"] = new Function
                    {
                        Body = "Events | where Timestamp > ago(1h)",
                        Folder = "Analytics",
                        DocString = "Gets events from the last hour",
                        Parameters = "()"
                    }
                }
            };

            // Act & Assert
            database.Name.Should().Be("TestDatabase");
            database.Team.Should().Be("DataEngineering");
            database.Admins.Should().HaveCount(1);
            database.Tables.Should().ContainKey("Events");
            database.Functions.Should().ContainKey("GetRecentEvents");

            var eventsTable = database.Tables["Events"];
            eventsTable.Columns.Should().HaveCount(3);
            eventsTable.Policies.Should().NotBeNull();
            eventsTable.Policies!.Retention.Should().Be("365d");

            var getRecentEventsFunction = database.Functions["GetRecentEvents"];
            getRecentEventsFunction.Body.Should().Contain("Events | where Timestamp > ago(1h)");
        }

        [Fact]
        public void Table_With_Policies_Should_Generate_Correct_Scripts()
        {
            // Arrange
            var table = new Table
            {
                Folder = "Analytics",
                DocString = "Processed events",
                Columns = new Dictionary<string, string>
                {
                    ["Id"] = "string",
                    ["ProcessedAt"] = "datetime"
                },
                Policies = new TablePolicy
                {
                    Retention = "90d",
                    HotCache = "7d",
                    RestrictedViewAccess = true,
                    UpdatePolicies = new List<UpdatePolicy>
                    {
                        new UpdatePolicy
                        {
                            Source = "RawEvents",
                            Query = "RawEvents | extend ProcessedAt = now()",
                            IsEnabled = true
                        }
                    }
                }
            };

            // Act
            var scripts = table.CreateScripts("ProcessedEvents", true);

            // Assert
            scripts.Should().NotBeEmpty();
            
            // Should have table creation script
            var createScript = scripts.FirstOrDefault(s => s.Kind == "CreateMergeTable");
            createScript.Should().NotBeNull();
            createScript!.Script.Text.Should().Contain(".create-merge table ProcessedEvents");
            createScript.Script.Text.Should().Contain("Id:string");
            createScript.Script.Text.Should().Contain("ProcessedAt:datetime");

            // Should have policy scripts
            var policyScripts = table.Policies.CreateScripts("ProcessedEvents");
            policyScripts.Should().NotBeEmpty();
            policyScripts.Should().Contain(s => s.Kind == "SoftDelete");
            policyScripts.Should().Contain(s => s.Kind == "HotCache");
            policyScripts.Should().Contain(s => s.Kind == "RestrictedViewAccess");
            policyScripts.Should().Contain(s => s.Kind == "TableUpdatePolicy");
        }

        [Fact]
        public void MaterializedView_Should_Generate_Complete_Scripts()
        {
            // Arrange
            var materializedView = new MaterializedView
            {
                Source = "Events",
                Query = "Events | summarize count() by bin(Timestamp, 1h), EventType",
                Folder = "Aggregations",
                DocString = "Hourly event type counts",
                Lookback = "7d",
                AutoUpdateSchema = true,
                Backfill = true,
                Policies = new Policy
                {
                    Retention = "180d",
                    HotCache = "14d"
                }
            };

            // Act
            var scripts = materializedView.CreateScripts("HourlyEventCounts", true);

            // Assert
            scripts.Should().NotBeEmpty();
            
            var createScript = scripts.FirstOrDefault(s => s.Kind == "CreateMaterializedViewAsync");
            createScript.Should().NotBeNull();
            createScript!.Script.Text.Should().Contain(".create async ifnotexists materialized-view");
            createScript.Script.Text.Should().Contain("HourlyEventCounts");
            createScript.Script.Text.Should().Contain("Events | summarize count() by bin(Timestamp, 1h), EventType");
        }

        [Fact]
        public void Function_Should_Handle_Complex_Parameters()
        {
            // Arrange
            var function = new Function
            {
                Body = "Events | where Timestamp >= startTime and Timestamp <= endTime | summarize count() by bin(Timestamp, interval)",
                Folder = "Analytics",
                DocString = "Counts events in time range with specified interval",
                Parameters = "startTime:datetime, endTime:datetime, interval:timespan", // Remove the problematic T:(*) parameter
                SkipValidation = false,
                View = false
            };

            // Act
            var scripts = function.CreateScripts("CountEventsInRange", true);

            // Assert
            scripts.Should().HaveCount(1);
            var script = scripts.First();
            script.Kind.Should().Be("CreateOrAlterFunction");
            script.Script.Text.Should().Contain(".create-or-alter function");
            script.Script.Text.Should().Contain("CountEventsInRange");
            script.Script.Text.Should().Contain("Events | where Timestamp >= startTime");
            script.Script.Text.Should().Contain("Folder=```Analytics```");
        }

        [Fact]
        public void ContinuousExport_Should_Generate_Proper_Script()
        {
            // Arrange
            var continuousExport = new ContinuousExport
            {
                ExternalTable = "ExternalEvents",
                Query = "Events | where Timestamp > ago(1h)",
                ManagedIdentity = "system",
                IntervalBetweenRuns = 30, // 30 minutes
                ForcedLatencyInMinutes = 5,
                SizeLimit = 1000000,
                Distributed = true
            };

            // Act
            var scripts = continuousExport.CreateScripts("HourlyEventExport", true);

            // Assert
            scripts.Should().HaveCount(1);
            var script = scripts.First();
            script.Kind.Should().Be("ContinuousExport");
            script.Script.Text.Should().Contain(".create-or-alter continuous-export HourlyEventExport");
            script.Script.Text.Should().Contain("to table ExternalEvents");
            script.Script.Text.Should().Contain("managedIdentity='system'");
            script.Script.Text.Should().Contain("distributed=True");
        }

        [Fact]
        public void ExternalTable_Should_Support_Multiple_Configurations()
        {
            // Arrange
            var externalTable = new ExternalTable
            {
                Kind = "storage",
                DataFormat = "parquet",
                ConnectionString = "https://storage.azure.com/container",
                PathFormat = "year={yyyy}/month={MM}/day={dd}",
                Schema = new Dictionary<string, string>
                {
                    ["EventId"] = "string",
                    ["Timestamp"] = "datetime",
                    ["Value"] = "real"
                },
                DocString = "External parquet files",
                Folder = "External",
                Compressed = true
            };

            // Act
            var scripts = externalTable.CreateScripts("ExternalEvents", true);

            // Assert
            scripts.Should().HaveCount(1);
            var script = scripts.First();
            script.Kind.Should().Be("External Table");
            script.Script.Text.Should().Contain(".create-or-alter external table ExternalEvents");
            script.Script.Text.Should().Contain("EventId:string");
            script.Script.Text.Should().Contain("Timestamp:datetime");
            script.Script.Text.Should().Contain("Value:real");
        }
    }
}
