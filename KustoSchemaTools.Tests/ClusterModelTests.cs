using KustoSchemaTools.Model;
using Xunit;
using FluentAssertions;

namespace KustoSchemaTools.Tests
{
    public class ClusterModelTests
    {
        [Fact]
        public void ClusterWorkloadGroup_Should_Generate_Correct_Script()
        {
            // Arrange
            var workloadGroup = new ClusterWorkloadGroup
            {
                Name = "TestGroup",
                RequestLimitsQueryCount = 100,
                RequestLimitsQueryCpuSecondsPerHour = TimeSpan.FromHours(2),
                RequestClassificationPolicyQueryWeightPercent = 50,
                RequestClassificationPolicyDatabases = new List<string> { "Database1", "Database2" }
            };

            // Act
            var script = workloadGroup.CreateScript();

            // Assert
            script.Should().NotBeNull();
            script.Kind.Should().Be("ClusterWorkloadGroup");
            script.Order.Should().Be(30);
            script.Text.Should().Contain(".create-or-alter workload_group TestGroup");
            script.Text.Should().Contain("QueryCount");
            script.Text.Should().Contain("QueryCpuSecondsPerHour");
            script.Text.Should().Contain("QueryWeightPercent");
            script.Text.Should().Contain("Database1");
        }

        [Fact]
        public void ClusterPolicy_Should_Generate_Multiple_Scripts()
        {
            // Arrange
            var policy = new ClusterPolicy
            {
                QueryWeakConsistencyEnabled = true,
                QueryThrottlingConcurrentQueriesLimit = 50,
                CalloutPolicyEnabled = true,
                CalloutPolicyAllowedDomains = new List<string> { "example.com", "test.com" }
            };

            // Act
            var scripts = policy.CreateScripts();

            // Assert
            scripts.Should().NotBeEmpty();
            scripts.Should().HaveCount(3);
            
            scripts[0].Kind.Should().Be("QueryWeakConsistency");
            scripts[0].Text.Should().Contain(".alter cluster policy query_weak_consistency");
            
            scripts[1].Kind.Should().Be("QueryThrottling");
            scripts[1].Text.Should().Contain(".alter cluster policy query_throttling");
            
            scripts[2].Kind.Should().Be("Callout");
            scripts[2].Text.Should().Contain(".alter cluster policy callout");
            scripts[2].Text.Should().Contain("example.com");
        }

        [Fact]
        public void Cluster_Should_Generate_All_Scripts_In_Order()
        {
            // Arrange
            var cluster = new Cluster
            {
                Name = "TestCluster",
                Url = "https://test.kusto.windows.net",
                Policy = new ClusterPolicy
                {
                    QueryWeakConsistencyEnabled = true
                },
                WorkloadGroups = new List<ClusterWorkloadGroup>
                {
                    new ClusterWorkloadGroup 
                    { 
                        Name = "Group1", 
                        RequestLimitsQueryCount = 10 
                    },
                    new ClusterWorkloadGroup 
                    { 
                        Name = "Group2", 
                        RequestLimitsQueryCount = 20 
                    }
                },
                Scripts = new List<DatabaseScript>
                {
                    new DatabaseScript(".show cluster", 100)
                }
            };

            // Act
            var allScripts = cluster.GenerateAllScripts();

            // Assert
            allScripts.Should().NotBeEmpty();
            allScripts.Should().HaveCount(4); // 1 policy + 2 workload groups + 1 custom script
            
            // Should be ordered by Order property
            allScripts[0].Order.Should().Be(20); // Policy script
            allScripts[1].Order.Should().Be(30); // First workload group
            allScripts[2].Order.Should().Be(30); // Second workload group  
            allScripts[3].Order.Should().Be(100); // Custom script
        }

        [Fact]
        public void Cluster_Should_Generate_Deletion_Scripts()
        {
            // Arrange
            var cluster = new Cluster();
            var workloadGroupsToDelete = new List<string> { "OldGroup1", "OldGroup2" };

            // Act
            var deletionScripts = cluster.GenerateDeletionScripts(workloadGroupsToDelete);

            // Assert
            deletionScripts.Should().HaveCount(2);
            deletionScripts[0].Text.Should().Contain(".drop workload_group OldGroup1");
            deletionScripts[1].Text.Should().Contain(".drop workload_group OldGroup2");
        }
    }
}
