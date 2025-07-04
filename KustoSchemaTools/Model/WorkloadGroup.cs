using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

namespace KustoSchemaTools.Model
{
    public enum RateLimitKind
    {
        ConcurrentRequests,
        ResourceUtilization
    }

    public enum RateLimitScope
    {
        WorkloadGroup,
        Principal
    }

    public enum QueriesEnforcementLevel
    {
        QueryHead,
        Cluster
    }

    public enum CommandsEnforcementLevel
    {
        Cluster,
        Database
    }

    public enum QueryConsistency
    {
        Strong,
        Weak,
        WeakAffinitizedByQuery,
        WeakAffinitizedByDatabase
    }

    public class WorkloadGroup : IEquatable<WorkloadGroup>
    {
        public required string WorkloadGroupName { get; set; }

        public WorkloadGroupPolicy? WorkloadGroupPolicy { get; set; }
        public bool Equals(WorkloadGroup? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return
                WorkloadGroupName == other.WorkloadGroupName &&
                EqualityComparer<WorkloadGroupPolicy?>.Default.Equals(WorkloadGroupPolicy, other.WorkloadGroupPolicy);
        }

        public override bool Equals(object? obj) => Equals(obj as WorkloadGroup);
        public override int GetHashCode()
        {
            var hc = new HashCode();
            hc.Add(WorkloadGroupName);
            hc.Add(WorkloadGroupPolicy);
            return hc.ToHashCode();
        }

        public string ToUpdateScript()
        {
            if (WorkloadGroupPolicy == null)
            {
                throw new InvalidOperationException("WorkloadGroupPolicy cannot be null when generating update script");
            }
            
            var workloadGroupPolicyJson = WorkloadGroupPolicy.ToJson();
            var script = $".alter-merge workload_group {WorkloadGroupName} ```{workloadGroupPolicyJson}```";
            return script;
        }
    }

    public class WorkloadGroupPolicy : IEquatable<WorkloadGroupPolicy>
    {
        [JsonProperty("RequestLimitsPolicy")]
        public RequestLimitsPolicy? RequestLimitsPolicy { get; set; }

        [JsonProperty("RequestRateLimitPolicies")]
        public List<RequestRateLimitPolicy>? RequestRateLimitPolicies { get; set; }

        [JsonProperty("RequestRateLimitsEnforcementPolicy")]
        public RequestRateLimitsEnforcementPolicy? RequestRateLimitsEnforcementPolicy { get; set; }

        [JsonProperty("QueryConsistencyPolicy")]
        public QueryConsistencyPolicy? QueryConsistencyPolicy { get; set; }

        public bool Equals(WorkloadGroupPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return EqualityComparer<RequestLimitsPolicy?>.Default.Equals(RequestLimitsPolicy, other.RequestLimitsPolicy) &&
                   (RequestRateLimitPolicies == null && other.RequestRateLimitPolicies == null ||
                    RequestRateLimitPolicies != null && other.RequestRateLimitPolicies != null &&
                    RequestRateLimitPolicies.SequenceEqual(other.RequestRateLimitPolicies)) &&
                   EqualityComparer<RequestRateLimitsEnforcementPolicy?>.Default.Equals(RequestRateLimitsEnforcementPolicy, other.RequestRateLimitsEnforcementPolicy) &&
                   EqualityComparer<QueryConsistencyPolicy?>.Default.Equals(QueryConsistencyPolicy, other.QueryConsistencyPolicy);
        }

        public override bool Equals(object? obj) => Equals(obj as WorkloadGroupPolicy);

        public override int GetHashCode()
        {
            var hc = new HashCode();
            hc.Add(RequestLimitsPolicy);
            hc.Add(RequestRateLimitsEnforcementPolicy);
            hc.Add(QueryConsistencyPolicy);
            if (RequestRateLimitPolicies != null)
            {
                foreach (var policy in RequestRateLimitPolicies)
                {
                    hc.Add(policy);
                }
            }
            return hc.ToHashCode();
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.Indented
            });
        }
    }

    public class PolicyValue<T> : IEquatable<PolicyValue<T>>
    {
        [JsonProperty("IsRelaxable")]
        public bool IsRelaxable { get; set; }

        [JsonProperty("Value")]
        public T? Value { get; set; }

        public bool Equals(PolicyValue<T>? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return IsRelaxable == other.IsRelaxable &&
                   EqualityComparer<T?>.Default.Equals(Value, other.Value);
        }

        public override bool Equals(object? obj) => Equals(obj as PolicyValue<T>);

        public override int GetHashCode()
        {
            return HashCode.Combine(IsRelaxable, Value);
        }
    }

    public class RequestLimitsPolicy : IEquatable<RequestLimitsPolicy>
    {
        [JsonProperty("DataScope")]
        public PolicyValue<string>? DataScope { get; set; }

        [JsonProperty("MaxMemoryPerQueryPerNode")]
        public PolicyValue<long>? MaxMemoryPerQueryPerNode { get; set; }

        [JsonProperty("MaxMemoryPerIterator")]
        public PolicyValue<long>? MaxMemoryPerIterator { get; set; }

        [JsonProperty("MaxFanoutThreadsPercentage")]
        public PolicyValue<int>? MaxFanoutThreadsPercentage { get; set; }

        [JsonProperty("MaxFanoutNodesPercentage")]
        public PolicyValue<int>? MaxFanoutNodesPercentage { get; set; }

        [JsonProperty("MaxResultRecords")]
        public PolicyValue<long>? MaxResultRecords { get; set; }

        [JsonProperty("MaxResultBytes")]
        public PolicyValue<long>? MaxResultBytes { get; set; }

        [JsonProperty("MaxExecutionTime")]
        public PolicyValue<TimeSpan>? MaxExecutionTime { get; set; }

        [JsonProperty("QueryResultsProgressiveUpdatePeriod")]
        public PolicyValue<TimeSpan>? QueryResultsProgressiveUpdatePeriod { get; set; }

        public bool Equals(RequestLimitsPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return EqualityComparer<PolicyValue<string>?>.Default.Equals(DataScope, other.DataScope) &&
                   EqualityComparer<PolicyValue<long>?>.Default.Equals(MaxMemoryPerQueryPerNode, other.MaxMemoryPerQueryPerNode) &&
                   EqualityComparer<PolicyValue<long>?>.Default.Equals(MaxMemoryPerIterator, other.MaxMemoryPerIterator) &&
                   EqualityComparer<PolicyValue<int>?>.Default.Equals(MaxFanoutThreadsPercentage, other.MaxFanoutThreadsPercentage) &&
                   EqualityComparer<PolicyValue<int>?>.Default.Equals(MaxFanoutNodesPercentage, other.MaxFanoutNodesPercentage) &&
                   EqualityComparer<PolicyValue<long>?>.Default.Equals(MaxResultRecords, other.MaxResultRecords) &&
                   EqualityComparer<PolicyValue<long>?>.Default.Equals(MaxResultBytes, other.MaxResultBytes) &&
                   EqualityComparer<PolicyValue<TimeSpan>?>.Default.Equals(MaxExecutionTime, other.MaxExecutionTime) &&
                   EqualityComparer<PolicyValue<TimeSpan>?>.Default.Equals(QueryResultsProgressiveUpdatePeriod, other.QueryResultsProgressiveUpdatePeriod);
        }

        public override bool Equals(object? obj) => Equals(obj as RequestLimitsPolicy);

        public override int GetHashCode()
        {
            var hc = new HashCode();
            hc.Add(DataScope);
            hc.Add(MaxMemoryPerQueryPerNode);
            hc.Add(MaxMemoryPerIterator);
            hc.Add(MaxFanoutThreadsPercentage);
            hc.Add(MaxFanoutNodesPercentage);
            hc.Add(MaxResultRecords);
            hc.Add(MaxResultBytes);
            hc.Add(MaxExecutionTime);
            hc.Add(QueryResultsProgressiveUpdatePeriod);
            return hc.ToHashCode();
        }
    }

    public class RequestRateLimitPolicy : IEquatable<RequestRateLimitPolicy>
    {
        [JsonProperty("IsEnabled")]
        public bool IsEnabled { get; set; }

        [JsonProperty("Scope")]
        [JsonConverter(typeof(Newtonsoft.Json.Converters.StringEnumConverter))]
        public RateLimitScope Scope { get; set; }

        [JsonProperty("LimitKind")]
        [JsonConverter(typeof(Newtonsoft.Json.Converters.StringEnumConverter))]
        public RateLimitKind LimitKind { get; set; }

        [JsonProperty("Properties")]
        public object Properties { get; set; } = new();

        public bool Equals(RequestRateLimitPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return IsEnabled == other.IsEnabled &&
                   Scope == other.Scope &&
                   LimitKind == other.LimitKind &&
                   EqualityComparer<object>.Default.Equals(Properties, other.Properties);
        }

        public override bool Equals(object? obj) => Equals(obj as RequestRateLimitPolicy);

        public override int GetHashCode()
        {
            return HashCode.Combine(IsEnabled, Scope, LimitKind, Properties);
        }
    }

    public class RequestRateLimitsEnforcementPolicy : IEquatable<RequestRateLimitsEnforcementPolicy>
    {
        [JsonProperty("QueriesEnforcementLevel")]
        [JsonConverter(typeof(Newtonsoft.Json.Converters.StringEnumConverter))]
        public QueriesEnforcementLevel QueriesEnforcementLevel { get; set; }

        [JsonProperty("CommandsEnforcementLevel")]
        [JsonConverter(typeof(Newtonsoft.Json.Converters.StringEnumConverter))]
        public CommandsEnforcementLevel CommandsEnforcementLevel { get; set; }

        public bool Equals(RequestRateLimitsEnforcementPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return QueriesEnforcementLevel == other.QueriesEnforcementLevel &&
                   CommandsEnforcementLevel == other.CommandsEnforcementLevel;
        }

        public override bool Equals(object? obj) => Equals(obj as RequestRateLimitsEnforcementPolicy);

        public override int GetHashCode()
        {
            return HashCode.Combine(QueriesEnforcementLevel, CommandsEnforcementLevel);
        }
    }

    public class QueryConsistencyPolicy : IEquatable<QueryConsistencyPolicy>
    {
        [JsonProperty("QueryConsistency")]
        public PolicyValue<QueryConsistency>? QueryConsistency { get; set; }

        [JsonProperty("CachedResultsMaxAge")]
        public PolicyValue<TimeSpan>? CachedResultsMaxAge { get; set; }

        public bool Equals(QueryConsistencyPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return EqualityComparer<PolicyValue<QueryConsistency>?>.Default.Equals(QueryConsistency, other.QueryConsistency) &&
                   EqualityComparer<PolicyValue<TimeSpan>?>.Default.Equals(CachedResultsMaxAge, other.CachedResultsMaxAge);
        }

        public override bool Equals(object? obj) => Equals(obj as QueryConsistencyPolicy);

        public override int GetHashCode()
        {
            return HashCode.Combine(QueryConsistency, CachedResultsMaxAge);
        }
    }
}