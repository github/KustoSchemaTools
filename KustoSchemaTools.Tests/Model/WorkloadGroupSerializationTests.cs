using KustoSchemaTools.Model;
using Newtonsoft.Json.Linq;

namespace KustoSchemaTools.Tests.Serialization
{
    public class WorkloadGroupSerializationTests
    {
        [Fact]
        public void ResourceKind_ShouldSerializeAsString_NotInteger()
        {
            // Arrange - matches the exact scenario from github/data#10509
            var policy = new WorkloadGroupPolicy
            {
                RequestRateLimitPolicies = new PolicyList<RequestRateLimitPolicy>
                {
                    new RequestRateLimitPolicy
                    {
                        IsEnabled = true,
                        Scope = RateLimitScope.Principal,
                        LimitKind = RateLimitKind.ResourceUtilization,
                        Properties = new RateLimitProperties
                        {
                            ResourceKind = RateLimitResourceKind.TotalCpuSeconds,
                            MaxUtilization = 36000,
                            TimeWindow = TimeSpan.FromMinutes(15)
                        }
                    }
                }
            };

            // Act
            var json = policy.ToJson();
            var parsed = JObject.Parse(json);

            // Assert - ResourceKind must be a string, not an integer
            var resourceKind = parsed["RequestRateLimitPolicies"]![0]!["Properties"]!["ResourceKind"]!;
            Assert.Equal(JTokenType.String, resourceKind.Type);
            Assert.Equal("TotalCpuSeconds", resourceKind.Value<string>());
        }

        [Fact]
        public void ResourceKind_RequestCount_ShouldSerializeAsString()
        {
            var policy = new WorkloadGroupPolicy
            {
                RequestRateLimitPolicies = new PolicyList<RequestRateLimitPolicy>
                {
                    new RequestRateLimitPolicy
                    {
                        IsEnabled = true,
                        Scope = RateLimitScope.WorkloadGroup,
                        LimitKind = RateLimitKind.ResourceUtilization,
                        Properties = new RateLimitProperties
                        {
                            ResourceKind = RateLimitResourceKind.RequestCount,
                            MaxUtilization = 100,
                            TimeWindow = TimeSpan.FromMinutes(1)
                        }
                    }
                }
            };

            var json = policy.ToJson();
            var parsed = JObject.Parse(json);

            var resourceKind = parsed["RequestRateLimitPolicies"]![0]!["Properties"]!["ResourceKind"]!;
            Assert.Equal(JTokenType.String, resourceKind.Type);
            Assert.Equal("RequestCount", resourceKind.Value<string>());
        }

        [Fact]
        public void AllEnumProperties_ShouldSerializeAsStrings()
        {
            // Arrange - a policy with every enum populated
            var policy = new WorkloadGroupPolicy
            {
                RequestRateLimitPolicies = new PolicyList<RequestRateLimitPolicy>
                {
                    new RequestRateLimitPolicy
                    {
                        IsEnabled = true,
                        Scope = RateLimitScope.Principal,
                        LimitKind = RateLimitKind.ResourceUtilization,
                        Properties = new RateLimitProperties
                        {
                            ResourceKind = RateLimitResourceKind.TotalCpuSeconds,
                            MaxUtilization = 36000,
                            TimeWindow = TimeSpan.FromMinutes(15)
                        }
                    }
                },
                RequestRateLimitsEnforcementPolicy = new RequestRateLimitsEnforcementPolicy
                {
                    QueriesEnforcementLevel = QueriesEnforcementLevel.QueryHead,
                    CommandsEnforcementLevel = CommandsEnforcementLevel.Database
                },
                QueryConsistencyPolicy = new QueryConsistencyPolicy
                {
                    QueryConsistency = new PolicyValue<QueryConsistency>
                    {
                        Value = QueryConsistency.WeakAffinitizedByDatabase,
                        IsRelaxable = true
                    }
                }
            };

            // Act
            var json = policy.ToJson();
            var parsed = JObject.Parse(json);

            // Assert - every enum value is a string
            var rateLimitPolicy = parsed["RequestRateLimitPolicies"]![0]!;
            Assert.Equal("Principal", rateLimitPolicy["Scope"]!.Value<string>());
            Assert.Equal("ResourceUtilization", rateLimitPolicy["LimitKind"]!.Value<string>());
            Assert.Equal("TotalCpuSeconds", rateLimitPolicy["Properties"]!["ResourceKind"]!.Value<string>());

            var enforcement = parsed["RequestRateLimitsEnforcementPolicy"]!;
            Assert.Equal("QueryHead", enforcement["QueriesEnforcementLevel"]!.Value<string>());
            Assert.Equal("Database", enforcement["CommandsEnforcementLevel"]!.Value<string>());

            var consistency = parsed["QueryConsistencyPolicy"]!["QueryConsistency"]!;
            Assert.Equal("WeakAffinitizedByDatabase", consistency["Value"]!.Value<string>());
        }

        [Fact]
        public void ToCreateScript_ShouldContainStringEnumValues()
        {
            // Arrange - the exact scenario from the issue
            var workloadGroup = new WorkloadGroup
            {
                WorkloadGroupName = "test-group",
                WorkloadGroupPolicy = new WorkloadGroupPolicy
                {
                    RequestRateLimitPolicies = new PolicyList<RequestRateLimitPolicy>
                    {
                        new RequestRateLimitPolicy
                        {
                            IsEnabled = true,
                            Scope = RateLimitScope.Principal,
                            LimitKind = RateLimitKind.ResourceUtilization,
                            Properties = new RateLimitProperties
                            {
                                ResourceKind = RateLimitResourceKind.TotalCpuSeconds,
                                MaxUtilization = 36000,
                                TimeWindow = TimeSpan.FromMinutes(15)
                            }
                        }
                    }
                }
            };

            // Act
            var script = workloadGroup.ToCreateScript();

            // Assert - the script should contain string value, not integer
            Assert.Contains("\"ResourceKind\": \"TotalCpuSeconds\"", script);
            Assert.DoesNotContain("\"ResourceKind\": 1", script);
            Assert.Contains("\"Scope\": \"Principal\"", script);
            Assert.Contains("\"LimitKind\": \"ResourceUtilization\"", script);
        }
    }
}
