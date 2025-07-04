using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class WorkloadGroup : IEquatable<WorkloadGroup>
    {
        public required string WorkloadGroupName { get; set; }

        public WorkloadGroupPolicy WorkloadGroupPolicy { get; set; }
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
            var workloadGroupPolicyJson = WorkloadGroupPolicy.ToJson();
            var script = $".alter-merge workload_group {WorkloadGroupName} ```{workloadGroupPolicyJson}```";
            return script;
        }
    }

    public class WorkloadGroupPolicy : IEquatable<WorkloadGroupPolicy>
    {
        [JsonProperty("RequestLimitsPolicy")]
        public RequestLimitsPolicy? RequestLimitsPolicy { get; set; }

        public bool Equals(WorkloadGroupPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return EqualityComparer<RequestLimitsPolicy?>.Default.Equals(RequestLimitsPolicy, other.RequestLimitsPolicy);
        }

        public override bool Equals(object? obj) => Equals(obj as WorkloadGroupPolicy);

        public override int GetHashCode()
        {
            return RequestLimitsPolicy?.GetHashCode() ?? 0;
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
        /// <summary>
        /// Limits the data scope that the query is allowed to reference
        /// </summary>
        [JsonProperty("DataScope")]
        public PolicyValue<string>? DataScope { get; set; }

        /// <summary>
        /// Maximum amount of memory a single query operator may allocate per node (in bytes)
        /// </summary>
        [JsonProperty("MaxMemoryPerQueryPerNode")]
        public PolicyValue<long>? MaxMemoryPerQueryPerNode { get; set; }

        /// <summary>
        /// Maximum amount of memory a single query operator iterator can allocate (in bytes)
        /// </summary>
        [JsonProperty("MaxMemoryPerIterator")]
        public PolicyValue<long>? MaxMemoryPerIterator { get; set; }

        /// <summary>
        /// Maximum percentage of total fanout threads in the cluster that a query can utilize
        /// </summary>
        [JsonProperty("MaxFanoutThreadsPercentage")]
        public PolicyValue<int>? MaxFanoutThreadsPercentage { get; set; }

        /// <summary>
        /// Maximum percentage of nodes in the cluster that a query can fanout to
        /// </summary>
        [JsonProperty("MaxFanoutNodesPercentage")]
        public PolicyValue<int>? MaxFanoutNodesPercentage { get; set; }

        /// <summary>
        /// Maximum number of records a query is allowed to return to the caller
        /// </summary>
        [JsonProperty("MaxResultRecords")]
        public PolicyValue<long>? MaxResultRecords { get; set; }

        /// <summary>
        /// Maximum amount of data a query is allowed to return to the caller (in bytes)
        /// </summary>
        [JsonProperty("MaxResultBytes")]
        public PolicyValue<long>? MaxResultBytes { get; set; }

        /// <summary>
        /// Maximum amount of time a request may execute
        /// </summary>
        [JsonProperty("MaxExecutionTime")]
        public PolicyValue<TimeSpan>? MaxExecutionTime { get; set; }

        /// <summary>
        /// How frequently progress of query results is reported (only takes effect if query results are progressive)
        /// </summary>
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
}